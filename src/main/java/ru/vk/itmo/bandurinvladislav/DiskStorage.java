package ru.vk.itmo.bandurinvladislav;

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
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

public class DiskStorage {
    private static final String INDEX_FILE_NAME = "index.idx";
    private static final String INDEX_TMP_FILE_NAME = "index.tmp";

    public static Iterator<Entry<MemorySegment>> range(
            Iterator<Entry<MemorySegment>> firstIterator,
            Iterator<Entry<MemorySegment>> secondIterator,
            List<MemorySegment> segmentList,
            MemorySegment from,
            MemorySegment to) {
        List<Iterator<Entry<MemorySegment>>> iterators = new ArrayList<>(segmentList.size() + 1);
        for (MemorySegment memorySegment : segmentList) {
            iterators.add(iterator(memorySegment, from, to));
        }
        iterators.add(firstIterator);
        iterators.add(secondIterator);

        return new MergeIterator<>(iterators, Comparator.comparing(Entry::key, PersistentDao::compare)) {
            @Override
            protected boolean skip(Entry<MemorySegment> memorySegmentEntry) {
                return memorySegmentEntry.value() == null;
            }
        };
    }

    public static MemorySegment save(Arena arena, Path storagePath, Iterable<Entry<MemorySegment>> iterable)
            throws IOException {
        final Path indexTmp = storagePath.resolve(INDEX_TMP_FILE_NAME);
        final Path indexFile = storagePath.resolve(INDEX_FILE_NAME);

        try {
            Files.createFile(indexFile);
        } catch (FileAlreadyExistsException ignored) {
            // it is ok, actually it is normal state
        }
        List<String> existedFiles = Files.readAllLines(indexFile, StandardCharsets.UTF_8);

        String newFileName = String.valueOf(existedFiles.size());

        MemorySegment fileSegment = fillSSTable(arena, storagePath.resolve(newFileName), iterable);
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
        return fileSegment;
    }

    public static MemorySegment compact(Arena arena, Path storagePath, Iterator<Entry<MemorySegment>> mergeIterator)
            throws IOException {
        if (!mergeIterator.hasNext()) {
            return null;
        }

        Path indexFile = storagePath.resolve(INDEX_FILE_NAME);

        if (!Files.exists(indexFile)) {
            throw new IllegalStateException("Unexpected missing file index.idx");
        }

        Path newFilePath = storagePath.resolve("compactedTable");

        ArrayList<Entry<MemorySegment>> compactedValues = new ArrayList<>();
        while (mergeIterator.hasNext()) {
            compactedValues.add(mergeIterator.next());
        }

        List<String> existedFiles = Files.readAllLines(indexFile, StandardCharsets.UTF_8);
        MemorySegment fileSegment = fillSSTable(arena, newFilePath, compactedValues);
        Files.writeString(
                indexFile,
                newFilePath.getFileName().toString(),
                StandardOpenOption.WRITE,
                StandardOpenOption.CREATE,
                StandardOpenOption.TRUNCATE_EXISTING
        );
        for (String existedFile : existedFiles) {
            Files.deleteIfExists(storagePath.resolve(existedFile));
        }
        Files.move(newFilePath,
                storagePath.resolve("0"),
                StandardCopyOption.ATOMIC_MOVE,
                StandardCopyOption.REPLACE_EXISTING);
        Files.writeString(
                indexFile,
                "0",
                StandardOpenOption.WRITE,
                StandardOpenOption.CREATE,
                StandardOpenOption.TRUNCATE_EXISTING
        );
        Files.deleteIfExists(newFilePath);
        return fileSegment;
    }

    private static MemorySegment fillSSTable(Arena arena, Path newFilePath, Iterable<Entry<MemorySegment>> iterable)
            throws IOException {
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

        if (count == 0) {
            return null;
        }

        // According to our simplified implementation of DB,
        // I rely on the fact that 'count' is always less than Integer.MAX_VALUE
        BloomFilter bloom = BloomFilter.createBloom((int) count);

        for (Entry<MemorySegment> entry : iterable) {
            bloom.add(entry.key());
        }

        // 8 bytes for size + actual filter_size
        long bloomSize = (long) bloom.getFilterSize() * Long.BYTES + Long.BYTES;

        long indexSize = count * 2 * Long.BYTES;

        try (FileChannel fileChannel = FileChannel.open(
                newFilePath,
                StandardOpenOption.WRITE,
                StandardOpenOption.READ,
                StandardOpenOption.CREATE
        );
        ) {
            MemorySegment fileSegment = fileChannel.map(
                    FileChannel.MapMode.READ_WRITE,
                    0,
                    indexSize == 0 ? Long.BYTES : bloomSize + indexSize + dataSize,
                    arena
            );

            // bloom:
            // |bloom_size|long_value1|long_value2|long_value3|...
            // long values stored in bloom filter - representation of bits
            // bloomSize = index start 
            fileSegment.set(ValueLayout.JAVA_LONG_UNALIGNED, 0, bloomSize);
            long bloomOffset = Long.BYTES;
            for (Long hash : bloom.getFilter().getLongs()) {
                fileSegment.set(ValueLayout.JAVA_LONG_UNALIGNED, bloomOffset, hash);
                bloomOffset += Long.BYTES;
            }

            // index:
            // |key0_Start|value0_Start|key1_Start|value1_Start|key2_Start|value2_Start|...
            // key0_Start = data start = end of index
            long dataOffset = bloomSize + indexSize;
            long indexOffset = bloomSize;
            for (Entry<MemorySegment> entry : iterable) {
                fileSegment.set(ValueLayout.JAVA_LONG_UNALIGNED, indexOffset, dataOffset);
                dataOffset += entry.key().byteSize();
                indexOffset += Long.BYTES;

                MemorySegment value = entry.value();
                if (value == null) {
                    fileSegment.set(ValueLayout.JAVA_LONG_UNALIGNED, indexOffset, StorageUtil.tombstone(dataOffset));
                } else {
                    fileSegment.set(ValueLayout.JAVA_LONG_UNALIGNED, indexOffset, dataOffset);
                    dataOffset += value.byteSize();
                }
                indexOffset += Long.BYTES;
            }

            // data:
            // |key0|value0|key1|value1|...
            dataOffset = bloomSize + indexSize;
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
            return fileSegment;
        }
    }

    public static List<MemorySegment> loadOrRecover(Path storagePath, Arena arena) throws IOException {
        Path indexTmp = storagePath.resolve(INDEX_TMP_FILE_NAME);
        Path indexFile = storagePath.resolve(INDEX_FILE_NAME);

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

    private static Iterator<Entry<MemorySegment>> iterator(MemorySegment page, MemorySegment from, MemorySegment to) {
        long recordIndexFrom = from == null ? 0 : StorageUtil.normalize(StorageUtil.indexOf(page, from));
        long bloomSize = StorageUtil.bloomSize(page);
        long recordIndexTo = to == null
                ? StorageUtil.recordsCount(page, bloomSize)
                : StorageUtil.normalize(StorageUtil.indexOf(page, to));
        long recordsCount = StorageUtil.recordsCount(page, bloomSize);

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
                MemorySegment key = StorageUtil
                        .slice(
                                page,
                                StorageUtil.startOfKey(page, index, bloomSize),
                                StorageUtil.endOfKey(page, index, bloomSize)
                        );
                long startOfValue = StorageUtil.startOfValue(page, index, bloomSize);
                MemorySegment value =
                        startOfValue < 0
                                ? null
                                : StorageUtil.slice(
                                page,
                                startOfValue,
                                StorageUtil.endOfValue(page, index, recordsCount, bloomSize)
                        );
                index++;
                return new BaseEntry<>(key, value);
            }
        };
    }

    private DiskStorage() {}
}
