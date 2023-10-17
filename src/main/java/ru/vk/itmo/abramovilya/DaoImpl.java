package ru.vk.itmo.abramovilya;

import ru.vk.itmo.BaseEntry;
import ru.vk.itmo.Config;
import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.*;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class DaoImpl implements Dao<MemorySegment, Entry<MemorySegment>> {
    private final ConcurrentNavigableMap<MemorySegment, Entry<MemorySegment>> map =
            new ConcurrentSkipListMap<>(DaoImpl::compareMemorySegments);
    private final Path storagePath;
    private final Arena arena = Arena.ofShared();
    private final String sstableBaseName = "storage";
    private final String indexBaseName = "table";
    private final Path metaFilePath;

    private final List<FileChannel> sstableFileChannels = new ArrayList<>();
    private final List<MemorySegment> sstableMappedList = new ArrayList<>();
    private final List<FileChannel> indexFileChannels = new ArrayList<>();
    private final List<MemorySegment> indexMappedList = new ArrayList<>();

    public DaoImpl(Config config) {
        storagePath = config.basePath();

        metaFilePath = storagePath.resolve("meta");
        try {
            if (!Files.exists(metaFilePath)) {
                Files.writeString(metaFilePath, "0", StandardOpenOption.CREATE, StandardOpenOption.WRITE);
            }

            int totalSSTables = Integer.parseInt(Files.readString(metaFilePath));
            for (int sstableNum = 0; sstableNum < totalSSTables; sstableNum++) {
                Path sstablePath = storagePath.resolve(sstableBaseName + (sstableNum + 1));
                Path indexPath = storagePath.resolve(indexBaseName + (sstableNum + 1));

                FileChannel sstableFileChannel = FileChannel.open(sstablePath, StandardOpenOption.READ);
                sstableFileChannels.add(sstableFileChannel);
                MemorySegment sstableMapped = sstableFileChannel.map(FileChannel.MapMode.READ_ONLY, 0, Files.size(sstablePath), arena);
                sstableMappedList.add(sstableMapped);

                FileChannel indexFileChannel = FileChannel.open(indexPath, StandardOpenOption.READ);
                indexFileChannels.add(indexFileChannel);
                MemorySegment indexMapped = indexFileChannel.map(FileChannel.MapMode.READ_ONLY, 0, Files.size(indexPath), arena);
                indexMappedList.add(indexMapped);
            }

        } catch (IOException e) {
//            throw new UncheckedIOException(e);
        }
    }

    @Override
    public Iterator<Entry<MemorySegment>> get(MemorySegment from, MemorySegment to) {
        return new Iterator<>() {
            private final PriorityQueue<Table> priorityQueue = new PriorityQueue<>();

            {
                ConcurrentNavigableMap<MemorySegment, Entry<MemorySegment>> subMap = getSubMap();
                try {
                    int totalSStables = getTotalSStables();
                    for (int i = totalSStables; i > 0; i--) {
                        long offset = findOffsetInIndex(from, to, i);
                        if (offset != -1) {
                            priorityQueue.add(new SSTable(i, offset, sstableMappedList.get(i - 1), indexMappedList.get(i - 1)));
                        }
                    }
                    if (!subMap.isEmpty()) {
                        priorityQueue.add(new MemTable(subMap));
                    }
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            }

            private ConcurrentNavigableMap<MemorySegment, Entry<MemorySegment>> getSubMap() {
                ConcurrentNavigableMap<MemorySegment, Entry<MemorySegment>> subMap;
                if (from == null && to == null) {
                    subMap = map;
                } else if (from == null) {
                    subMap = map.headMap(to);
                } else if (to == null) {
                    subMap = map.tailMap(from);
                } else {
                    subMap = map.subMap(from, to);
                }
                return subMap;
            }

            @Override
            public boolean hasNext() {
                cleanUpSStableQueue();
                return !priorityQueue.isEmpty();
            }

            @Override
            public Entry<MemorySegment> next() {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }

                Table minTable = priorityQueue.remove();
                MemorySegment key = minTable.getKey();
                MemorySegment value = minTable.getValue();
                removeExpiredValues(key);

                MemorySegment minPairNextKey = minTable.nextKey();
                if (minPairNextKey != null && (to == null || compareMemorySegments(minPairNextKey, to) < 0)) {
                    priorityQueue.add(minTable);
                }
                return new BaseEntry<>(key, value);
            }

            private void removeExpiredValues(MemorySegment minMemorySegment) {
                while (!priorityQueue.isEmpty() && priorityQueue.peek().getKey().mismatch(minMemorySegment) == -1) {
                    Table indexWithSameMin = priorityQueue.remove();
                    MemorySegment nextKey = indexWithSameMin.nextKey();
                    if (nextKey != null && (to == null || compareMemorySegments(nextKey, to) < 0)) {
                        priorityQueue.add(indexWithSameMin);
                    }
                }
            }

            private void cleanUpSStableQueue() {
                if (!priorityQueue.isEmpty()) {
                    Table minTable = priorityQueue.element();
                    MemorySegment key = minTable.getKey();
                    MemorySegment value = minTable.getValue();

                    if (value == null) {
                        priorityQueue.remove();
                        removeExpiredValues(key);
                        MemorySegment minPairNextKey = minTable.nextKey();
                        if (minPairNextKey != null && (to == null || compareMemorySegments(minPairNextKey, to) < 0)) {
                            priorityQueue.add(minTable);
                        }
                        cleanUpSStableQueue();
                    }
                }
            }
        };
    }

    // Finds offset in table file to key such
    // that it is greater or equal to from
    private long findOffsetInIndex(MemorySegment from, MemorySegment to, int i) throws IOException {
        long readOffset = 0;
        MemorySegment storageMapped = sstableMappedList.get(i - 1);
        MemorySegment indexMapped = indexMappedList.get(i - 1);

        if (from != null) {
            FoundSegmentIndexIndexValue found = upperBound(from, storageMapped, indexMapped, indexMapped.byteSize());
            MemorySegment foundMemorySegment = found.found();
            if (compareMemorySegments(foundMemorySegment, from) < 0 || to != null && compareMemorySegments(foundMemorySegment, to) >= 0) {
                return -1;
            }
            return found.index() * 2 * Long.BYTES + Long.BYTES;
        } else {

            if (to == null) {
                return Long.BYTES;
            } // from == null && to != null

            long firstKeySize = storageMapped.get(ValueLayout.JAVA_LONG_UNALIGNED, readOffset);
            readOffset += Long.BYTES;
            MemorySegment firstKey = storageMapped.asSlice(readOffset, firstKeySize);

            if (compareMemorySegments(firstKey, to) >= 0) {
                return -1;
            }
            return Long.BYTES;
        }
    }

    @Override
    public void upsert(Entry<MemorySegment> entry) {
        map.put(entry.key(), entry);
    }

    @Override
    public Entry<MemorySegment> get(MemorySegment key) {
        var value = map.get(key);
        if (value != null) {
            if (value.value() != null) {
                return value;
            }
            return null;
        }

        int totalSStables = getTotalSStables();
        for (int sstableNum = totalSStables; sstableNum > 0; sstableNum--) {
            var foundEntry = seekForValueInFile(key, sstableNum);
            if (foundEntry != null) {
                if (foundEntry.value() != null) {
                    return foundEntry;
                }
                return null;
            }
        }
        return null;
    }

    private Entry<MemorySegment> seekForValueInFile(MemorySegment key, int sstableNum) {
        Path storageFilePath = storagePath.resolve(sstableBaseName + sstableNum);

        if (!Files.exists(storageFilePath)) {
            return null;
        }

        MemorySegment storageMapped = sstableMappedList.get(sstableNum - 1);
        MemorySegment indexMapped = indexMappedList.get(sstableNum - 1);

        FoundSegmentIndexIndexValue found = upperBound(key, storageMapped, indexMapped, indexMapped.byteSize());
        if (compareMemorySegments(found.found(), key) != 0) {
            return null;
        } else {
            return getEntryFromIndexFile(storageMapped, indexMapped, found.index());
        }
    }

    private FoundSegmentIndexIndexValue upperBound(MemorySegment key, MemorySegment storageMapped, MemorySegment indexMapped, long indexSize) {
        long l = -1;
        long r = indexMapped.get(ValueLayout.JAVA_LONG_UNALIGNED, indexSize - 2 * Long.BYTES);

        while (r - l > 1) {
            long m = (r + l) / 2;
            MemorySegment ms = getKeyFromStorageFileAndEntryNum(storageMapped, indexMapped, m);

            if (compareMemorySegments(key, ms) > 0) {
                l = m;
            } else {
                r = m;
            }
        }

        MemorySegment found = getKeyFromStorageFileAndEntryNum(storageMapped, indexMapped, r);
        return new FoundSegmentIndexIndexValue(found, r);
    }

    private record FoundSegmentIndexIndexValue(MemorySegment found, long index) {
    }

    private Entry<MemorySegment> getEntryFromIndexFile(MemorySegment storageMapped, MemorySegment indexMapped, long entryNum) {
        long offsetInStorageFile = indexMapped.get(ValueLayout.JAVA_LONG_UNALIGNED, 2 * Long.BYTES * entryNum + Long.BYTES);

        long keySize = storageMapped.get(ValueLayout.JAVA_LONG_UNALIGNED, offsetInStorageFile);
        offsetInStorageFile += Long.BYTES;
        MemorySegment key = storageMapped.asSlice(offsetInStorageFile, keySize);
        offsetInStorageFile += keySize;

        long valueSize = storageMapped.get(ValueLayout.JAVA_LONG_UNALIGNED, offsetInStorageFile);
        offsetInStorageFile += Long.BYTES;
        MemorySegment value;
        if (valueSize == -1) {
            value = null;
        } else {
            value = storageMapped.asSlice(offsetInStorageFile, valueSize);
        }
        return new BaseEntry<>(key, value);
    }

    private MemorySegment getKeyFromStorageFileAndEntryNum(MemorySegment storageMapped, MemorySegment indexMapped, long entryNum) {
        long offsetInStorageFile = indexMapped.get(ValueLayout.JAVA_LONG_UNALIGNED, 2 * Long.BYTES * entryNum + Long.BYTES);

        long msSize = storageMapped.get(ValueLayout.JAVA_LONG_UNALIGNED, offsetInStorageFile);
        return storageMapped.asSlice(offsetInStorageFile + Long.BYTES, msSize);
    }


    @Override
    public void flush() throws IOException {
        writeMapIntoFile();
        incTotalSStablesAmount();
    }

    private void incTotalSStablesAmount() throws IOException {
        int totalSStables = getTotalSStables();
        Files.writeString(metaFilePath, String.valueOf(totalSStables + 1));
    }

    @Override
    public void close() throws IOException {
        if (arena.scope().isAlive()) {
            arena.close();
        }
        flush();
        for (FileChannel fc : sstableFileChannels) {
            if (fc.isOpen()) fc.close();
        }
        for (FileChannel fc : indexFileChannels) {
            if (fc.isOpen()) fc.close();
        }
    }

    private void writeMapIntoFile() throws IOException {
        if (map.isEmpty()) {
            return;
        }

        int currSStableNum = getTotalSStables() + 1;
        Path sstablePath = storagePath.resolve(sstableBaseName + currSStableNum);
        Path indexPath = storagePath.resolve(indexBaseName + currSStableNum);

        long storageWriteOffset = 0;
        long indexWriteOffset = 0;
        try (var storageChannel = FileChannel.open(sstablePath,
                StandardOpenOption.READ,
                StandardOpenOption.WRITE,
                StandardOpenOption.CREATE);

             var indexChannel = FileChannel.open(indexPath,
                     StandardOpenOption.READ,
                     StandardOpenOption.WRITE,
                     StandardOpenOption.CREATE);

             var storageArena = Arena.ofConfined();
             var indexArena = Arena.ofConfined()) {

            MemorySegment mappedStorage = storageChannel.map(FileChannel.MapMode.READ_WRITE, 0, calcMapByteSizeInFile(), storageArena);
            MemorySegment mappedIndex = indexChannel.map(FileChannel.MapMode.READ_WRITE, 0, calcIndexByteSizeInFile(), indexArena);

            long entryNum = 0;
            for (var entry : map.values()) {
                mappedIndex.set(ValueLayout.JAVA_LONG_UNALIGNED, indexWriteOffset, entryNum);
                indexWriteOffset += Long.BYTES;
                mappedIndex.set(ValueLayout.JAVA_LONG_UNALIGNED, indexWriteOffset, storageWriteOffset);
                indexWriteOffset += Long.BYTES;
                entryNum++;

                writeMemorySegment(entry.key(), mappedStorage, storageWriteOffset);
                storageWriteOffset += Long.BYTES;
                storageWriteOffset += entry.key().byteSize();
                writeMemorySegment(entry.value(), mappedStorage, storageWriteOffset);
                storageWriteOffset += Long.BYTES;
                if (entry.value() != null) storageWriteOffset += entry.value().byteSize();
            }
            mappedStorage.load();
        }
    }

    private int getTotalSStables() {
        return sstableFileChannels.size();
    }

    private long calcIndexByteSizeInFile() {
        return map.size() * Long.BYTES * 2L;
    }

    private long calcMapByteSizeInFile() {
        long size = 0;
        for (var entry : map.values()) {
            size += 2 * Long.BYTES;
            size += entry.key().byteSize();
            if (entry.value() != null) {
                size += entry.value().byteSize();
            }
        }
        return size;
    }

    // Every memorySegment in file has the following structure:
    // 8 bytes - size, <size> bytes - value
    // If memorySegment has the size of -1 byte, then it means its value is DELETED
    private void writeMemorySegment(MemorySegment memorySegment, MemorySegment mapped, long writeOffset) {
        if (memorySegment != null) {
            long msSize = memorySegment.byteSize();
            mapped.set(ValueLayout.JAVA_LONG_UNALIGNED, writeOffset, msSize);
            writeOffset += Long.BYTES;
            MemorySegment.copy(memorySegment, 0, mapped, writeOffset, msSize);
        } else {
            mapped.set(ValueLayout.JAVA_LONG_UNALIGNED, writeOffset, -1);
        }
    }

    public static int compareMemorySegments(MemorySegment segment1, MemorySegment segment2) {
        long offset = segment1.mismatch(segment2);
        if (offset == -1) {
            return 0;
        } else if (offset == segment1.byteSize()) {
            return -1;
        } else if (offset == segment2.byteSize()) {
            return 1;
        }
        return Byte.compare(
                segment1.get(ValueLayout.JAVA_BYTE, offset),
                segment2.get(ValueLayout.JAVA_BYTE, offset)
        );
    }
}
