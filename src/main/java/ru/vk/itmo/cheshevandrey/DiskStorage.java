package ru.vk.itmo.cheshevandrey;

import ru.vk.itmo.BaseEntry;
import ru.vk.itmo.Entry;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.file.Files;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.StandardCopyOption;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

public class DiskStorage {

    private final List<MemorySegment> segmentList;

    private static final Arena indexArena = Arena.ofShared();

    private static final String EXTENSION = ".sst";
    private static final String DELIMITER = "_";
    private static final String INDEX_NAME = "index.idx";

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

        return new MergeIterator<>(iterators, Comparator.comparing(Entry::key, InMemoryDao::compare)) {
            @Override
            protected boolean skip(Entry<MemorySegment> memorySegmentEntry) {
                return memorySegmentEntry.value() == null;
            }
        };
    }

    public static void save(Path storagePath, Iterable<Entry<MemorySegment>> iterable)
            throws IOException {

        MemorySegment indexSegment = loadIndexFile(storagePath);

        int compactNumber = indexSegment.get(ValueLayout.JAVA_INT_UNALIGNED, 0);
        int ssTablesNumber = indexSegment.get(ValueLayout.JAVA_INT_UNALIGNED, Integer.BYTES);

        String newFileName = getFileName(ssTablesNumber, compactNumber);

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

        updateIndex(indexSegment, compactNumber, ssTablesNumber + 1);
    }

    public static List<MemorySegment> loadOrRecover(Path storagePath, Arena arena) throws IOException {

        MemorySegment indexSegment = loadIndexFile(storagePath);

        int compactNumber = indexSegment.get(ValueLayout.JAVA_INT_UNALIGNED, 0);
        int ssTablesNumber = indexSegment.get(ValueLayout.JAVA_INT_UNALIGNED, Integer.BYTES);

        List<MemorySegment> result = new ArrayList<>(ssTablesNumber);
        for (int i = 0; i < ssTablesNumber; i++) {
            Path file = storagePath.resolve(getFileName(i, compactNumber));
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

    public static MemorySegment loadIndexFile(Path storagePath) throws IOException {
        Path indexPath = storagePath.resolve(INDEX_NAME);
        MemorySegment indexSegment;
        boolean isFileExists = true;

        try {
            Files.createFile(indexPath);
            isFileExists = false;
        } catch (FileAlreadyExistsException ignored) {
            // it is ok, actually it is normal state
        }

        try (FileChannel fileChannel = FileChannel.open(indexPath, StandardOpenOption.READ, StandardOpenOption.WRITE)) {
            indexSegment = fileChannel.map(
                    FileChannel.MapMode.READ_WRITE,
                    0,
                    Integer.BYTES * 2,
                    indexArena
            );
            if (!isFileExists) {
                updateIndex(indexSegment, 0, 0);
            }
            return indexSegment;
        }
    }

    private static void updateIndex(MemorySegment indexSegment, int compactNumber, int ssTablesCount) {
        indexSegment.set(ValueLayout.JAVA_INT_UNALIGNED, 0, compactNumber);
        indexSegment.set(ValueLayout.JAVA_INT_UNALIGNED, Integer.BYTES, ssTablesCount);
    }

    public void compact(Path storagePath, Iterable<Entry<MemorySegment>> iterableMemTable) throws IOException {
        IterableStorage storage = new IterableStorage(iterableMemTable, this);
        if (!storage.iterator().hasNext()) {
            return;
        }

        save(storagePath, storage);
        updateStateAfterCompact(storagePath);
    }

    private static void updateStateAfterCompact(Path storagePath) throws IOException {
        MemorySegment indexSegment = loadIndexFile(storagePath);

        int compactNumber = indexSegment.get(ValueLayout.JAVA_INT_UNALIGNED, 0);
        int ssTablesNumber = indexSegment.get(ValueLayout.JAVA_INT_UNALIGNED, Integer.BYTES);

        for (int i = 0; i < ssTablesNumber - 1; i++) {
            String ssTableName = getFileName(i, compactNumber);
            Files.delete(storagePath.resolve(ssTableName));
        }

        String lastFileName = getFileName(ssTablesNumber - 1, compactNumber);
        String newCompactFileName = getFileName(0, compactNumber + 1);

        Files.move(storagePath.resolve(lastFileName), storagePath.resolve(newCompactFileName),
                StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);

        updateIndex(indexSegment, compactNumber + 1, 1);
    }

    private static final class IterableStorage implements Iterable<Entry<MemorySegment>> {

        Iterable<Entry<MemorySegment>> iterableMemTable;
        DiskStorage diskStorage;

        private IterableStorage(Iterable<Entry<MemorySegment>> iterableMemTable, DiskStorage diskStorage) {
            this.iterableMemTable = iterableMemTable;
            this.diskStorage = diskStorage;
        }

        @Override
        public Iterator<Entry<MemorySegment>> iterator() {
            return diskStorage.range(iterableMemTable.iterator(), null, null);
        }
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

    private static String getFileName(int ssTableNumber, int compactNumber) {
        return ssTableNumber + DELIMITER + compactNumber + EXTENSION;
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
