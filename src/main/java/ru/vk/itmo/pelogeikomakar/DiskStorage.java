package ru.vk.itmo.pelogeikomakar;

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
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Stream;

public class DiskStorage {

    public static final String SSTABLE_PREFIX = "sstable_";
    private final List<MemorySegment> segmentList;
    private final Lock filesLock = new ReentrantLock();
    private final AtomicReference<Iterable<Entry<MemorySegment>>> flushingValues = new AtomicReference<>(Collections.emptyList());

    public DiskStorage(List<MemorySegment> segmentList) {
        this.segmentList = segmentList;
    }

    public MergeIterator range(
            Iterator<Entry<MemorySegment>> firstIterator,
            MemorySegment from,
            MemorySegment to) {

        return rangeSegments(segmentList, firstIterator, flushingValues.get().iterator(), from, to);
    }

    private static MergeIterator rangeSegments(List<MemorySegment> memSegList,
                                               Iterator<Entry<MemorySegment>> firstIterator,
                                               Iterator<Entry<MemorySegment>> secondIterator,
                                               MemorySegment from,
                                               MemorySegment to) {
        List<Iterator<Entry<MemorySegment>>> iterators = new ArrayList<>(memSegList.size() + 1);
        for (MemorySegment memorySegment : memSegList) {
            iterators.add(iterator(memorySegment, from, to));
        }
        iterators.add(secondIterator);
        iterators.add(firstIterator);
        return new MergeIterator(iterators, Comparator.comparing(Entry::key, ConcurrentDao::compare)) {
            @Override
            protected boolean shouldSkip(Entry<MemorySegment> memorySegmentEntry) {
                return memorySegmentEntry.value() == null;
            }
        };
    }

    public AtomicReference<Iterable<Entry<MemorySegment>>> getFlushingValues() {
        return flushingValues;
    }

    public void saveNextSSTable(Path storagePath, Iterable<Entry<MemorySegment>> iterable, Arena arenaShared)
            throws IOException {

        // for more safety
        flushingValues.setRelease(iterable);

        final Path indexTmp = storagePath.resolve("indexFlush.tmp");
        final Path indexFile = storagePath.resolve("index.idx");

        try {
            Files.createFile(indexFile);
        } catch (FileAlreadyExistsException ignored) {
            // it is ok, actually it is normal state
        }

        String newFileName = SSTABLE_PREFIX + "Flush";

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

        filesLock.lock();
        try {
            List<String> existedFiles = Files.readAllLines(indexFile, StandardCharsets.UTF_8);
            String flushedFile = SSTABLE_PREFIX + existedFiles.size();
            List<String> list = new ArrayList<>(existedFiles.size() + 1);
            list.addAll(existedFiles);
            list.add(flushedFile);
            Files.write(
                    indexTmp,
                    list,
                    StandardOpenOption.WRITE,
                    StandardOpenOption.CREATE,
                    StandardOpenOption.TRUNCATE_EXISTING
            );
            Files.deleteIfExists(indexFile);
            Files.move(indexTmp, indexFile, StandardCopyOption.ATOMIC_MOVE);
            Files.move(storagePath.resolve(newFileName), storagePath.resolve(flushedFile), StandardCopyOption.ATOMIC_MOVE);
            segmentList.add(openTable(flushedFile, storagePath, arenaShared));
            flushingValues.setRelease(Collections.emptyList());
        } finally {
            filesLock.unlock();
        }

    }

    public void compact(Path storagePath, Arena arenaShared)
            throws IOException {

        // for more safety - no one can make flush until compact ends
        filesLock.lock();
        try {
            List<MemorySegment> memSegList = segmentList;
            Iterable<Entry<MemorySegment>> iterable = () ->
                    rangeSegments(memSegList, Collections.emptyIterator(),
                            Collections.emptyIterator(), null, null);
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

            Files.move(
                    compactionTmpFile,
                    compactionFile(storagePath),
                    StandardCopyOption.ATOMIC_MOVE,
                    StandardCopyOption.REPLACE_EXISTING
            );
            String fileName = finalizeCompaction(storagePath, true);
            segmentList.add(openTable(fileName, storagePath, arenaShared));
        } finally {
            filesLock.unlock();
        }
    }

    public MemorySegment openTable(String tableName, Path storagePath, Arena arena) throws IOException {
        Path file = storagePath.resolve(tableName);
        try (FileChannel fileChannel = FileChannel.open(file, StandardOpenOption.READ, StandardOpenOption.WRITE)) {
            return fileChannel.map(
                    FileChannel.MapMode.READ_WRITE,
                    0,
                    Files.size(file),
                    arena
            );
        }
    }

    private static String finalizeCompaction(Path storagePath, boolean isGood) throws IOException {
        Path compactionFile = compactionFile(storagePath);
        Path indexTmp = storagePath.resolve("indexFinalizeComp.tmp");
        Path indexFile = storagePath.resolve("index.idx");

        if (isGood) {
            List<String> existedFiles = Files.readAllLines(indexFile, StandardCharsets.UTF_8);
            for (String fileName : existedFiles) {
                try {
                    Files.delete(storagePath.resolve(fileName));
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            }
        } else {
            try (Stream<Path> stream = Files.find(storagePath, 1, (path, attrs) -> path.getFileName().toString().startsWith(SSTABLE_PREFIX))) {
                stream.forEach(p -> {
                    try {
                        Files.delete(p);
                    } catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                });
            }
        }

        Files.deleteIfExists(indexFile);
        Files.deleteIfExists(indexTmp);

        boolean noData = Files.size(compactionFile) == 0;
        String newTableName = SSTABLE_PREFIX + "0";

        Files.write(
                indexTmp,
                noData ? Collections.emptyList() : Collections.singleton(newTableName),
                StandardOpenOption.WRITE,
                StandardOpenOption.CREATE,
                StandardOpenOption.TRUNCATE_EXISTING
        );

        Files.move(indexTmp, indexFile, StandardCopyOption.ATOMIC_MOVE);
        if (noData) {
            Files.delete(compactionFile);
            return "";
        } else {
            Files.move(compactionFile, storagePath.resolve(newTableName), StandardCopyOption.ATOMIC_MOVE);
            return newTableName;
        }
    }

    private static Path compactionFile(Path storagePath) {
        return storagePath.resolve("compaction");
    }


    public static List<MemorySegment> loadOrRecover(Path storagePath, Arena arena) throws IOException {
        if (Files.exists(compactionFile(storagePath))) {
            finalizeCompaction(storagePath, false);
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

    public static long recordsCount(MemorySegment segment) {
        long indexSize = indexSize(segment);
        return indexSize / Long.BYTES / 2;
    }

    private static long indexSize(MemorySegment segment) {
        return segment.get(ValueLayout.JAVA_LONG_UNALIGNED, 0);
    }

    private static Iterator<Entry<MemorySegment>> iterator(MemorySegment page, MemorySegment from, MemorySegment to) {
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

    public static long startOfKey(MemorySegment segment, long recordIndex) {
        return segment.get(ValueLayout.JAVA_LONG_UNALIGNED, recordIndex * 2 * Long.BYTES);
    }

    public static long endOfKey(MemorySegment segment, long recordIndex) {
        return normalizedStartOfValue(segment, recordIndex);
    }

    private static long normalizedStartOfValue(MemorySegment segment, long recordIndex) {
        return normalize(startOfValue(segment, recordIndex));
    }

    public static long startOfValue(MemorySegment segment, long recordIndex) {
        return segment.get(ValueLayout.JAVA_LONG_UNALIGNED, recordIndex * 2 * Long.BYTES + Long.BYTES);
    }

    public static long endOfValue(MemorySegment segment, long recordIndex, long recordsCount) {
        if (recordIndex < recordsCount - 1) {
            return startOfKey(segment, recordIndex + 1);
        }
        return segment.byteSize();
    }

    private static long tombstone(long offset) {
        return 1L << 63 | offset;
    }

    public static long normalize(long value) {
        return value & ~(1L << 63);
    }

}