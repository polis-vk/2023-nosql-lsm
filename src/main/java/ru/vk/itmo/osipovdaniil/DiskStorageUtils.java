package ru.vk.itmo.osipovdaniil;

import ru.vk.itmo.BaseEntry;
import ru.vk.itmo.Entry;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.*;
import java.util.stream.Stream;

public final class DiskStorageUtils {

    public static final String SSTABLE_PREFIX = "sstable_";
    private static final String INDEX = "index.idx";
    private static final String INDEX_TMP = "index.tmp";

    private DiskStorageUtils() {
    }

    public static Path getIndexTmpPath(final Path storagePath) {
        return storagePath.resolve(INDEX_TMP);
    }

    public static Path getIndexPath(final Path storagePath) {
        return storagePath.resolve(INDEX);
    }

    public static List<MemorySegment> loadOrRecover(final Path storagePath, final Arena arena) throws IOException {
        if (Files.exists(compactionFile(storagePath))) {
            finalizeCompaction(storagePath);
        }
        final Path indexTmp = getIndexTmpPath(storagePath);
        final Path indexFile = getIndexPath(storagePath);
        if (!Files.exists(indexFile)) {
            if (Files.exists(indexTmp)) {
                Files.move(indexTmp, indexFile, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
            } else {
                Files.createFile(indexFile);
            }
        }
        final List<String> existedFiles = Files.readAllLines(indexFile, StandardCharsets.UTF_8);
        final List<MemorySegment> result = new ArrayList<>(existedFiles.size());
        for (String fileName : existedFiles) {
            final Path file = storagePath.resolve(fileName);
            try (FileChannel fileChannel = FileChannel.open(file, StandardOpenOption.READ, StandardOpenOption.WRITE)) {
                MemorySegment fileSegment = fileChannel.map(
                        FileChannel.MapMode.READ_WRITE,
                        0,
                        Files.size(file),
                        arena);
                result.add(fileSegment);
            }
        }
        return result;
    }

    private static long indexOf(final MemorySegment segment, final MemorySegment key) {
        final long recordsCount = recordsCount(segment);

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

    private static long recordsCount(final MemorySegment segment) {
        long indexSize = indexSize(segment);
        return indexSize / Long.BYTES / 2;
    }

    private static long indexSize(final MemorySegment segment) {
        return segment.get(ValueLayout.JAVA_LONG_UNALIGNED, 0);
    }

    static Iterator<Entry<MemorySegment>> iterator(final MemorySegment page,
                                                   final MemorySegment from,
                                                   final MemorySegment to) {
        long recordIndexFrom = from == null ? 0 : normalize(indexOf(page, from));
        long recordIndexTo = to == null ? recordsCount(page) : normalize(indexOf(page, to));
        final long recordsCount = recordsCount(page);
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

    private static MemorySegment slice(final MemorySegment page, final long start, final long end) {
        return page.asSlice(start, end - start);
    }

    private static long startOfKey(final MemorySegment segment, final long recordIndex) {
        return segment.get(ValueLayout.JAVA_LONG_UNALIGNED, recordIndex * 2 * Long.BYTES);
    }

    private static long endOfKey(final MemorySegment segment, final long recordIndex) {
        return normalizedStartOfValue(segment, recordIndex);
    }

    private static long normalizedStartOfValue(final MemorySegment segment, final long recordIndex) {
        return normalize(startOfValue(segment, recordIndex));
    }

    private static long startOfValue(final MemorySegment segment, final long recordIndex) {
        return segment.get(ValueLayout.JAVA_LONG_UNALIGNED, recordIndex * 2 * Long.BYTES + Long.BYTES);
    }

    private static long endOfValue(final MemorySegment segment, final long recordIndex, final long recordsCount) {
        if (recordIndex < recordsCount - 1) {
            return startOfKey(segment, recordIndex + 1);
        }
        return segment.byteSize();
    }

    private static long tombstone(final long offset) {
        return 1L << 63 | offset;
    }

    private static long normalize(final long value) {
        return value & ~(1L << 63);
    }

    public static void save(final Path storagePath, final Iterable<Entry<MemorySegment>> iterable)
            throws IOException {
        final Path indexTmp = getIndexTmpPath(storagePath);
        final Path indexFile = getIndexPath(storagePath);

        try {
            Files.createFile(indexFile);
        } catch (FileAlreadyExistsException ignored) {
            // it is ok, actually it is normal state
        }
        final List<String> existedFiles = Files.readAllLines(indexFile, StandardCharsets.UTF_8);
        final String newFileName = SSTABLE_PREFIX + existedFiles.size();
        long dataSize = 0;
        long count = 0;
        for (Entry<MemorySegment> entry : iterable) {
            dataSize += entry.key().byteSize();
            MemorySegment value = entry.value();
            if (value != null) {
                dataSize += value.byteSize();
            }
            count++;
        }
        long indexSize = count * 2 * Long.BYTES;
        try (FileChannel fileChannel = FileChannel.open(
                storagePath.resolve(newFileName),
                StandardOpenOption.WRITE,
                StandardOpenOption.READ,
                StandardOpenOption.CREATE);
             Arena writeArena = Arena.ofConfined()) {
            MemorySegment fileSegment = fileChannel.map(
                    FileChannel.MapMode.READ_WRITE,
                    0,
                    indexSize + dataSize,
                    writeArena
            );
            // index:
            // |key0_Start|value0_Start|key1_Start|value1_Start|key2_Start|value2_Start|...
            // key0_Start = data start = end of index
            long dataOffset = indexSize;
            int indexOffset = 0;
            for (Entry<MemorySegment> entry : iterable) {
                fileSegment.set(ValueLayout.JAVA_LONG_UNALIGNED, indexOffset, dataOffset);
                dataOffset += entry.key().byteSize();
                indexOffset += Long.BYTES;

                MemorySegment value = entry.value();
                if (value == null) {
                    fileSegment.set(ValueLayout.JAVA_LONG_UNALIGNED, indexOffset, tombstone(dataOffset));
                } else {
                    fileSegment.set(ValueLayout.JAVA_LONG_UNALIGNED, indexOffset, dataOffset);
                    dataOffset += value.byteSize();
                }
                indexOffset += Long.BYTES;
            }
            // data:
            // |key0|value0|key1|value1|...
            dataOffset = indexSize;
            for (Entry<MemorySegment> entry : iterable) {
                MemorySegment key = entry.key();
                MemorySegment.copy(key, 0, fileSegment, dataOffset, key.byteSize());
                dataOffset += key.byteSize();

                MemorySegment value = entry.value();
                if (value != null) {
                    MemorySegment.copy(value, 0, fileSegment, dataOffset, value.byteSize());
                    dataOffset += value.byteSize();
                }
            }
        }
        final List<String> list = new ArrayList<>(existedFiles.size() + 1);
        list.addAll(existedFiles);
        list.add(newFileName);
        Files.write(indexTmp,
                list,
                StandardOpenOption.WRITE,
                StandardOpenOption.CREATE,
                StandardOpenOption.TRUNCATE_EXISTING);
        Files.deleteIfExists(indexFile);
        Files.move(indexFile, indexTmp, StandardCopyOption.ATOMIC_MOVE);
    }

    public static void compact(final Path storagePath, final Iterable<Entry<MemorySegment>> iterable)
            throws IOException {
        final String newFileName = "compaction.tmp";
        final Path compactionTmpFile = storagePath.resolve(newFileName);
        long dataSize = 0;
        long count = 0;
        for (Entry<MemorySegment> entry : iterable) {
            dataSize += entry.key().byteSize();
            MemorySegment value = entry.value();
            if (value != null) {
                dataSize += value.byteSize();
            }
            count++;
        }
        long indexSize = count * 2 * Long.BYTES;
        try (FileChannel fileChannel = FileChannel.open(
                compactionTmpFile,
                StandardOpenOption.WRITE,
                StandardOpenOption.READ,
                StandardOpenOption.CREATE);
                Arena writeArena = Arena.ofConfined()) {
            final MemorySegment fileSegment = fileChannel.map(
                    FileChannel.MapMode.READ_WRITE,
                    0,
                    indexSize + dataSize,
                    writeArena
            );

            // index:
            // |key0_Start|value0_Start|key1_Start|value1_Start|key2_Start|value2_Start|...
            // key0_Start = data start = end of index
            long dataOffset = indexSize;
            int indexOffset = 0;
            for (Entry<MemorySegment> entry : iterable) {
                fileSegment.set(ValueLayout.JAVA_LONG_UNALIGNED, indexOffset, dataOffset);
                dataOffset += entry.key().byteSize();
                indexOffset += Long.BYTES;

                MemorySegment value = entry.value();
                if (value == null) {
                    fileSegment.set(ValueLayout.JAVA_LONG_UNALIGNED, indexOffset, tombstone(dataOffset));
                } else {
                    fileSegment.set(ValueLayout.JAVA_LONG_UNALIGNED, indexOffset, dataOffset);
                    dataOffset += value.byteSize();
                }
                indexOffset += Long.BYTES;
            }

            // data:
            // |key0|value0|key1|value1|...
            dataOffset = indexSize;
            for (Entry<MemorySegment> entry : iterable) {
                MemorySegment key = entry.key();
                MemorySegment.copy(key, 0, fileSegment, dataOffset, key.byteSize());
                dataOffset += key.byteSize();

                MemorySegment value = entry.value();
                if (value != null) {
                    MemorySegment.copy(value, 0, fileSegment, dataOffset, value.byteSize());
                    dataOffset += value.byteSize();
                }
            }
        }
        Files.move(compactionTmpFile,
                storagePath.resolve("compaction"),
                StandardCopyOption.ATOMIC_MOVE,
                StandardCopyOption.REPLACE_EXISTING);
        finalizeCompaction(storagePath);
    }

    private static void finalizeCompaction(Path storagePath) throws IOException {
        Path compactionFile = compactionFile(storagePath);
        try (Stream<Path> stream = Files.find(storagePath, 1, (path, _) -> path.getFileName().toString().startsWith(SSTABLE_PREFIX))) {
            stream.forEach(p -> {
                try {
                    Files.delete(p);
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            });
        }

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
}
