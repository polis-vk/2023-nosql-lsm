package ru.vk.itmo.alenkovayulya;

import ru.vk.itmo.BaseEntry;
import ru.vk.itmo.Entry;
import ru.vk.itmo.alenkovayulya.bloomfilter.BloomFilter;
import ru.vk.itmo.alenkovayulya.bloomfilter.Utils;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;
import java.util.stream.Stream;

public class DiskStorage {

    public static final String SSTABLE_PREFIX = "sstable_";
    private final List<MemorySegment> segmentList;

    public DiskStorage(List<MemorySegment> segmentList) {
        this.segmentList = segmentList;
    }

    public Iterator<Entry<MemorySegment>> range(
            Iterator<Entry<MemorySegment>> firstIterator,
            MemorySegment from,
            MemorySegment to) {
        List<Iterator<Entry<MemorySegment>>> iterators = new ArrayList<>(segmentList.size() + 1);

        for (MemorySegment memorySegment : segmentList) {
            iterators.add(iterator(memorySegment, from, to));
        }
        iterators.add(firstIterator);

        return new MergeIterator<>(iterators, Comparator.comparing(Entry::key, AlenkovaDao::compare)) {
            @Override
            protected boolean shouldSkip(Entry<MemorySegment> memorySegmentEntry) {
                return memorySegmentEntry.value() == null;
            }
        };
    }

    public Iterator<Entry<MemorySegment>> rangeWithBloorFilter(
            Iterator<Entry<MemorySegment>> firstIterator,
            MemorySegment from,
            MemorySegment to) {
        List<Iterator<Entry<MemorySegment>>> iterators = new ArrayList<>(segmentList.size() + 1);

        for (MemorySegment memorySegment : segmentList) {
            if (checkBloom(memorySegment, from)) {
                iterators.add(iterator(memorySegment, from, to));
            }
        }
        iterators.add(firstIterator);

        return new MergeIterator<>(iterators, Comparator.comparing(Entry::key, AlenkovaDao::compare)) {
            @Override
            protected boolean shouldSkip(Entry<MemorySegment> memorySegmentEntry) {
                return memorySegmentEntry.value() == null;
            }
        };
    }

    public static void saveNextSSTable(Path storagePath, Iterable<Entry<MemorySegment>> iterable)
            throws IOException {
        final Path indexTmp = storagePath.resolve("index.tmp");
        final Path indexFile = storagePath.resolve("index.idx");

        try {
            Files.createFile(indexFile);
        } catch (FileAlreadyExistsException ignored) {
            // it is ok, actually it is normal state
        }
        List<String> existedFiles = Files.readAllLines(indexFile, StandardCharsets.UTF_8);

        String newFileName = SSTABLE_PREFIX + existedFiles.size();

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
            return;
        }

        BloomFilter bloom = BloomFilter.createBloom((int) count);

        for (Entry<MemorySegment> entry : iterable) {
            bloom.add(entry.key());
        }

        // 8 bytes for size + actual filter_size
        long bloomSize = (long) bloom.getFilterSize() * Long.BYTES + Long.BYTES;

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
                    bloomSize + indexSize + dataSize,
                    writeArena
            );

            // |bloom_size|long_value1|long_value2|long_value3|...
            fileSegment.set(ValueLayout.JAVA_LONG_UNALIGNED, 0, bloomSize);
            long bloomOffset = Long.BYTES;
            for (Long hash : bloom.getFilter().getLongs()) {
                fileSegment.set(ValueLayout.JAVA_LONG_UNALIGNED, bloomOffset, hash);
                bloomOffset += Long.BYTES;
            }

            // index:
            // |key0_Start|value0_Start|key1_Start|value1_Start|key2_Start|value2_Start|...
            // key0_Start = data start = end of index
            long dataOffset = bloomOffset + indexSize;
            long indexOffset = bloomOffset;
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
            dataOffset = bloomOffset + indexSize;
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

        List<String> list = new ArrayList<>(existedFiles.size() + 1);
        list.addAll(existedFiles);
        list.add(newFileName);
        Files.write(
                indexTmp,
                list,
                StandardOpenOption.WRITE,
                StandardOpenOption.CREATE,
                StandardOpenOption.TRUNCATE_EXISTING
        );

        Files.deleteIfExists(indexFile);

        Files.move(indexTmp, indexFile, StandardCopyOption.ATOMIC_MOVE);
    }

    public static void compact(Path storagePath, Iterable<Entry<MemorySegment>> iterable)
            throws IOException {

        String newFileName = "compaction.tmp";
        Path compactionTmpFile = storagePath.resolve(newFileName);


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
            return;
        }

        BloomFilter bloom = BloomFilter.createBloom((int) count);

        for (Entry<MemorySegment> entry : iterable) {
            bloom.add(entry.key());
        }

        // 8 bytes for size + actual filter_size
        long bloomSize = (long) bloom.getFilterSize() * Long.BYTES + Long.BYTES;
        long indexSize = count * 2 * Long.BYTES;

        try (
                FileChannel fileChannel = FileChannel.open(
                        compactionTmpFile,
                        StandardOpenOption.WRITE,
                        StandardOpenOption.READ,
                        StandardOpenOption.CREATE
                );
                Arena writeArena = Arena.ofConfined()
        ) {
            MemorySegment fileSegment = fileChannel.map(
                    FileChannel.MapMode.READ_WRITE,
                    0,
                    bloomSize + indexSize + dataSize,
                    writeArena
            );

            // |bloom_size|long_value1|long_value2|long_value3|...
            fileSegment.set(ValueLayout.JAVA_LONG_UNALIGNED, 0, bloomSize);
            long bloomOffset = Long.BYTES;
            for (Long hash : bloom.getFilter().getLongs()) {
                fileSegment.set(ValueLayout.JAVA_LONG_UNALIGNED, bloomOffset, hash);
                bloomOffset += Long.BYTES;
            }

            // index:
            // |key0_Start|value0_Start|key1_Start|value1_Start|key2_Start|value2_Start|...
            // key0_Start = data start = end of index
            long dataOffset = bloomOffset + indexSize;
            long indexOffset = bloomOffset;
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
        }

        Files.move(
                compactionTmpFile,
                storagePath.resolve("compaction"),
                StandardCopyOption.ATOMIC_MOVE,
                StandardCopyOption.REPLACE_EXISTING
        );

        finalizeCompaction(storagePath);
    }

    private static void finalizeCompaction(Path storagePath) throws IOException {
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

        Path indexTmp = storagePath.resolve("index.tmp");
        Path indexFile = storagePath.resolve("index.idx");

        Files.deleteIfExists(indexFile);
        Files.deleteIfExists(indexTmp);

        Path compactionFile = compactionFile(storagePath);
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
            finalizeCompaction(storagePath);
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
        long bloomSize = bloomSize(segment);
        long recordsCount = recordsCount(segment, bloomSize);

        long left = 0;
        long right = recordsCount - 1;
        while (left <= right) {
            long mid = (left + right) >>> 1;

            long startOfKey = startOfKey(segment, mid, bloomSize);
            long endOfKey = endOfKey(segment, mid, bloomSize);
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


    public static long recordsCount(MemorySegment segment, long bloomSize) {
        long indexSize = indexSize(segment, bloomSize);
        return indexSize / Long.BYTES / 2;
    }

    public static long bloomSize(MemorySegment segment) {
        return segment.get(ValueLayout.JAVA_LONG_UNALIGNED, 0);
    }

    public static long indexSize(MemorySegment segment, long bloomSize) {
        return segment.get(ValueLayout.JAVA_LONG_UNALIGNED, bloomSize) - bloomSize;
    }


    private static Iterator<Entry<MemorySegment>> iterator(MemorySegment page, MemorySegment from, MemorySegment to) {
        long recordIndexFrom = from == null ? 0 : normalize(indexOf(page, from));
        long bloomSize = bloomSize(page);

        long recordIndexTo = to == null ? recordsCount(page, bloomSize) : normalize(indexOf(page, to));
        long recordsCount = recordsCount(page, bloomSize);

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
                MemorySegment key = slice(page, startOfKey(page, index, bloomSize), endOfKey(page, index, bloomSize));
                long startOfValue = startOfValue(page, index, bloomSize);
                MemorySegment value =
                        startOfValue < 0
                                ? null
                                : slice(page, startOfValue, endOfValue(page, index, recordsCount, bloomSize));
                index++;
                return new BaseEntry<>(key, value);
            }
        };
    }

    public static MemorySegment slice(MemorySegment page, long start, long end) {
        return page.asSlice(start, end - start);
    }

    public static long startOfKey(MemorySegment segment, long recordIndex, long bloomSize) {
        return segment.get(ValueLayout.JAVA_LONG_UNALIGNED, bloomSize + recordIndex * 2 * Long.BYTES);
    }

    public static long endOfKey(MemorySegment segment, long recordIndex, long bloomSize) {
        return normalizedStartOfValue(segment, recordIndex, bloomSize);
    }

    public static long normalizedStartOfValue(MemorySegment segment, long recordIndex, long bloomSize) {
        return normalize(startOfValue(segment, recordIndex, bloomSize));
    }

    public static long startOfValue(MemorySegment segment, long recordIndex, long bloomSize) {
        return segment.get(ValueLayout.JAVA_LONG_UNALIGNED, bloomSize + recordIndex * 2 * Long.BYTES + Long.BYTES);
    }

    public static long endOfValue(MemorySegment segment, long recordIndex, long recordsCount, long bloomSize) {
        if (recordIndex < recordsCount - 1) {
            return startOfKey(segment, recordIndex + 1, bloomSize);
        }
        return segment.byteSize();
    }

    private static long tombstone(long offset) {
        return 1L << 63 | offset;
    }

    private static long normalize(long value) {
        return value & ~(1L << 63);
    }

    public static boolean checkBloom(MemorySegment page, MemorySegment key) {
        long bloomSize = bloomSize(page);
        int bitsetSize = (int) (bloomSize - Long.BYTES) * 8;
        int hashCount = Utils.countOptimalHashFunctions(bitsetSize, (int) recordsCount(page, bloomSize));
        long[] indexes = new long[hashCount];
        Utils.hashKey(key, indexes);
        BloomFilter.fillIndexes(indexes[1], indexes[0], hashCount, bitsetSize, indexes);

        return checkIndexes(page, indexes);
    }

    public static boolean checkIndexes(MemorySegment page, long[] indexes) {
        for (long index : indexes) {
            int pageOffset = (int) index >> 6;
            if (!checkBit(
                    page.get(ValueLayout.JAVA_LONG_UNALIGNED, Long.BYTES + pageOffset * Long.BYTES),
                    index - (pageOffset << 6)
            )) {
                return false;
            }
        }
        return true;
    }

    public static boolean checkBit(long value, long i) {
        return (value & (1L << (63 - i))) != 0;
    }

    public List<MemorySegment> getSegmentList() {
        return segmentList;
    }
}
