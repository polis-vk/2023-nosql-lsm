package ru.vk.itmo.kononovvladimir;

import ru.vk.itmo.BaseEntry;
import ru.vk.itmo.Entry;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.stream.Stream;

public final class StorageUtils {
    public static final String SSTABLE_PREFIX = "sstable_";

    private StorageUtils() {

    }

    public static void finalizeCompaction(Path storagePath, boolean doDelete) throws IOException {
        if (doDelete) {
            try (Stream<Path> stream =
                         Files.find(
                                 storagePath,
                                 1,
                                 (path, ignored) -> path.getFileName().toString().startsWith(SSTABLE_PREFIX))) {
                stream.forEach(p -> {
                    try {
                        Files.delete(p);
                    } catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                });
            }
        }

        Path compactionFile = compactionFile(storagePath);
        Path indexTmp = storagePath.resolve("index.tmp");
        Path indexFile = storagePath.resolve("index.idx");

        Files.deleteIfExists(indexFile);
        Files.deleteIfExists(indexTmp);

        boolean noData = Files.size(compactionFile) == 0;

        Files.write(
                indexTmp,
                noData ? Collections.emptyList() : Collections.singleton(SSTABLE_PREFIX + "0"),
                StandardOpenOption.WRITE,
                StandardOpenOption.CREATE,
                StandardOpenOption.TRUNCATE_EXISTING
        );

        Files.move(indexTmp, indexFile, StandardCopyOption.ATOMIC_MOVE);
        if (noData) {
            Files.delete(compactionFile);
        } else {
            Files.move(compactionFile, storagePath.resolve(SSTABLE_PREFIX + "0"), StandardCopyOption.ATOMIC_MOVE);
        }
    }

    private static Path compactionFile(Path storagePath) {
        return storagePath.resolve("compaction");
    }

    public static List<MemorySegment> loadOrRecover(Path storagePath, Arena arena) throws IOException {
        if (Files.exists(compactionFile(storagePath))) {
            finalizeCompaction(storagePath, true);
        }

        Path indexTmp = storagePath.resolve("index.tmp");
        Path indexFile = storagePath.resolve("index.idx");

        if (!Files.exists(indexFile)) {
            if (Files.exists(indexTmp)) {
                Files.move(indexTmp, indexFile, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
            } else {
                Files.createFile(indexFile);
            }
        }

        List<String> existedFiles = Files.readAllLines(indexFile, StandardCharsets.UTF_8);
        List<MemorySegment> result = new ArrayList<>(existedFiles.size());
        for (String fileName : existedFiles) {
            Path file = storagePath.resolve(fileName);
            try (FileChannel fileChannel = FileChannel.open(file, StandardOpenOption.READ, StandardOpenOption.WRITE)) {
                MemorySegment fileSegment = fileChannel.map(
                        FileChannel.MapMode.READ_WRITE,
                        0,
                        Files.size(file),
                        arena
                );
                result.add(fileSegment);
            }
        }

        return result;
    }

    public static long indexOf(MemorySegment segment, MemorySegment key) {
        long recordsCount = recordsCount(segment);

        long left = 0;
        long right = recordsCount - 1;
        while (left <= right) {
            long mid = (left + right) >>> 1;

            long startOfKey = startOfKey(segment, mid);
            long endOfKey = endOfKey(segment, mid);
            long mismatch = MemorySegment.mismatch(segment, startOfKey, endOfKey, key, 0, key.byteSize());
            if (mismatch == -1) {
                return mid;
            }

            if (mismatch == key.byteSize()) {
                right = mid - 1;
                continue;
            }

            if (mismatch == endOfKey - startOfKey) {
                left = mid + 1;
                continue;
            }

            int b1 = Byte.toUnsignedInt(segment.get(ValueLayout.JAVA_BYTE, startOfKey + mismatch));
            int b2 = Byte.toUnsignedInt(key.get(ValueLayout.JAVA_BYTE, mismatch));
            if (b1 > b2) {
                right = mid - 1;
            } else {
                left = mid + 1;
            }
        }

        return tombstone(left);
    }

    private static long recordsCount(MemorySegment segment) {
        long indexSize = indexSize(segment);
        return indexSize / Long.BYTES / 2;
    }

    private static long indexSize(MemorySegment segment) {
        return segment.get(ValueLayout.JAVA_LONG_UNALIGNED, 0);
    }

    public static Iterator<Entry<MemorySegment>> iterator(MemorySegment page, MemorySegment from, MemorySegment to) {
        long recordIndexFrom = from == null ? 0 : normalize(indexOf(page, from));
        long recordIndexTo = to == null ? recordsCount(page) : normalize(indexOf(page, to));
        long recordsCount = recordsCount(page);

        return new Iterator<>() {
            long index = recordIndexFrom;

            @Override
            public boolean hasNext() {
                return index < recordIndexTo;
            }

            @Override
            public Entry<MemorySegment> next() {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }
                MemorySegment key = slice(page, startOfKey(page, index), endOfKey(page, index));
                long startOfValue = startOfValue(page, index);
                MemorySegment value =
                        startOfValue < 0
                                ? null
                                : slice(page, startOfValue, endOfValue(page, index, recordsCount));
                index++;
                return new BaseEntry<>(key, value);
            }
        };
    }

    private static MemorySegment slice(MemorySegment page, long start, long end) {
        return page.asSlice(start, end - start);
    }

    private static long startOfKey(MemorySegment segment, long recordIndex) {
        return segment.get(ValueLayout.JAVA_LONG_UNALIGNED, recordIndex * 2 * Long.BYTES);
    }

    private static long endOfKey(MemorySegment segment, long recordIndex) {
        return normalizedStartOfValue(segment, recordIndex);
    }

    private static long normalizedStartOfValue(MemorySegment segment, long recordIndex) {
        return normalize(startOfValue(segment, recordIndex));
    }

    private static long startOfValue(MemorySegment segment, long recordIndex) {
        return segment.get(ValueLayout.JAVA_LONG_UNALIGNED, recordIndex * 2 * Long.BYTES + Long.BYTES);
    }

    private static long endOfValue(MemorySegment segment, long recordIndex, long recordsCount) {
        if (recordIndex < recordsCount - 1) {
            return startOfKey(segment, recordIndex + 1);
        }
        return segment.byteSize();
    }

    public static long tombstone(long offset) {
        return 1L << 63 | offset;
    }

    public static long normalize(long value) {
        return value & ~(1L << 63);
    }
}
