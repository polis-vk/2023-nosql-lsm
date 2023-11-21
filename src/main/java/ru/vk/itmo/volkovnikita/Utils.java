package ru.vk.itmo.volkovnikita;

import ru.vk.itmo.BaseEntry;
import ru.vk.itmo.Entry;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.stream.Stream;

import static java.nio.file.StandardCopyOption.ATOMIC_MOVE;

public class Utils {
    public static final String SSTABLE_PREFIX = "sstable_";
    public static final String INDEX_TMP_FILE = "index.tmp";
    public static final String INDEX_FILE = "index.idx";

    private Utils() {
    }

    public static void completeCompaction(Path directoryPath, List<Path> filesToRemove) throws IOException {
        if (filesToRemove.isEmpty()) {
            deleteSSTableFiles(directoryPath);
        } else {
            filesToRemove.forEach(FileDeleter::deleteFile);
        }

        Path compactionResultFile = Utils.compactionFile(directoryPath);
        Path temporaryIndexFile = directoryPath.resolve(Utils.INDEX_TMP_FILE);
        Path permanentIndexFile = directoryPath.resolve(Utils.INDEX_FILE);

        Files.deleteIfExists(permanentIndexFile);
        Files.deleteIfExists(temporaryIndexFile);

        boolean isCompactionFileEmpty = Files.size(compactionResultFile) == 0;

        writeIndexFile(temporaryIndexFile, isCompactionFileEmpty);
        Files.move(temporaryIndexFile, permanentIndexFile, ATOMIC_MOVE);

        if (isCompactionFileEmpty) {
            Files.delete(compactionResultFile);
        } else {
            Files.move(compactionResultFile, directoryPath.resolve(Utils.SSTABLE_PREFIX + "0"), ATOMIC_MOVE);
        }
    }

    private static void deleteSSTableFiles(Path path) throws IOException {
        try (Stream<Path> stableFiles = Files.find(path, 1,
                (filePath, attributes) -> filePath.getFileName().toString().startsWith(Utils.SSTABLE_PREFIX))) {
            stableFiles.forEach(FileDeleter::deleteFile);
        }
    }

    private static void writeIndexFile(Path indexFile, boolean isEmpty) throws IOException {
        List<String> file = isEmpty ? Collections.emptyList() : Collections.singletonList(Utils.SSTABLE_PREFIX + "0");
        Files.write(
                indexFile,
                file,
                StandardOpenOption.WRITE,
                StandardOpenOption.CREATE,
                StandardOpenOption.TRUNCATE_EXISTING
        );
    }

    private static class FileDeleter {

        private FileDeleter() {

        }

        static void deleteFile(Path file) {
            try {
                Files.delete(file);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }

    public static Path compactionFile(Path storagePath) {
        return storagePath.resolve("compaction");
    }

    private static long indexOf(MemorySegment segment, MemorySegment key) {
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

    private static long normalize(long value) {
        return value & ~(1L << 63);
    }
}
