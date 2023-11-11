package ru.vk.itmo.shemetovalexey;

import ru.vk.itmo.BaseEntry;
import ru.vk.itmo.Entry;

import java.io.IOException;
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
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

final class StorageUtils {
    private static final String TMP_DIRECTORY_NAME = "tmp";
    private static final String INDEX_FILE_NAME = "index";
    private static final String IDX_EXTENSION = ".idx";
    private static final String TMP_EXTENSION = ".tmp";

    private StorageUtils() {
    }

    public static void compact(Path path, Iterable<Entry<MemorySegment>> iterable) throws IOException {
        try {
            Files.createDirectory(path.resolve(TMP_DIRECTORY_NAME));
        } catch (IOException ignored) {
            // it is mean, that directory already exist
        }
        final Path tmpDirectory = path.resolve(TMP_DIRECTORY_NAME);
        save(tmpDirectory, iterable);
        final Path indexFile = path.resolve(INDEX_FILE_NAME + IDX_EXTENSION);
        List<String> existedFiles = Files.readAllLines(indexFile, StandardCharsets.UTF_8);
        for (String existedFile : existedFiles) {
            Files.delete(path.resolve(existedFile));
        }
        final Path indexTmp = tmpDirectory.resolve(INDEX_FILE_NAME + IDX_EXTENSION);
        Files.move(indexTmp, indexFile, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
        existedFiles = Files.readAllLines(indexFile, StandardCharsets.UTF_8);
        final Path dataTmp = tmpDirectory.resolve(existedFiles.get(0));
        final Path dataFile = path.resolve(existedFiles.get(0));
        Files.move(dataTmp, dataFile, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
    }

    public static void save(Path storagePath, Iterable<Entry<MemorySegment>> iterable)
            throws IOException {
        final Path indexTmp = storagePath.resolve(INDEX_FILE_NAME + TMP_EXTENSION);
        final Path indexFile = storagePath.resolve(INDEX_FILE_NAME + IDX_EXTENSION);

        try {
            Files.createFile(indexFile);
        } catch (FileAlreadyExistsException ignored) {
            // it is ok, actually it is normal state
        }
        List<String> existedFiles = Files.readAllLines(indexFile, StandardCharsets.UTF_8);

        String newFileName = String.valueOf(existedFiles.size());

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

        try (
                FileChannel fileChannel = FileChannel.open(
                        storagePath.resolve(newFileName),
                        StandardOpenOption.WRITE,
                        StandardOpenOption.READ,
                        StandardOpenOption.CREATE
                );
                Arena writeArena = Arena.ofConfined()
        ) {
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

        Files.move(indexFile, indexTmp, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);

        List<String> list = new ArrayList<>(existedFiles.size() + 1);
        list.addAll(existedFiles);
        list.add(newFileName);
        Files.write(
                indexFile,
                list,
                StandardOpenOption.WRITE,
                StandardOpenOption.CREATE,
                StandardOpenOption.TRUNCATE_EXISTING
        );

        Files.delete(indexTmp);
    }

    public static List<MemorySegment> loadOrRecover(Path storagePath, Arena arena) throws IOException {
        Path indexTmp = storagePath.resolve(INDEX_FILE_NAME + TMP_EXTENSION);
        Path indexFile = storagePath.resolve(INDEX_FILE_NAME + IDX_EXTENSION);

        if (Files.exists(indexTmp)) {
            Files.move(indexTmp, indexFile, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
        } else {
            try {
                Files.createFile(indexFile);
            } catch (FileAlreadyExistsException ignored) {
                // it is ok, actually it is normal state
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

    static Iterator<Entry<MemorySegment>> iterator(MemorySegment page, MemorySegment from, MemorySegment to) {
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

    private static long tombstone(long offset) {
        return 1L << 63 | offset;
    }

    private static long normalize(long value) {
        return value & ~(1L << 63);
    }

}
