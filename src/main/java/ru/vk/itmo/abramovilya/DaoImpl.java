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
import java.util.Comparator;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.PriorityQueue;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class DaoImpl implements Dao<MemorySegment, Entry<MemorySegment>> {
    private final ConcurrentNavigableMap<MemorySegment, Entry<MemorySegment>> map =
            new ConcurrentSkipListMap<>(DaoImpl::compareMemorySegments);
    private final Path storagePath;
    private final Arena arena = Arena.ofShared();
    private final String sstableBaseName = "storage";
    private final String indexBaseName = "index";
    private final String indexAboveIndexBaseName = "indexindex";
    private final Path metaFilePath;


    public DaoImpl(Config config) {
        storagePath = config.basePath();

        metaFilePath = storagePath.resolve("meta");
        try {
            if (!Files.exists(metaFilePath)) {
                Files.createFile(metaFilePath);
                Files.writeString(metaFilePath, "0");
            }
        } catch (IOException e) {
//            throw new UncheckedIOException(e);

        }
    }

    record Pair(MemorySegment memorySegment, Table index) {
    }

    @Override
    public Iterator<Entry<MemorySegment>> get(MemorySegment from, MemorySegment to) {
        return new Iterator<>() {
            private final PriorityQueue<Pair> priorityQueue = new PriorityQueue<>(
                    ((Comparator<Pair>) (p1, p2) -> compareMemorySegments(p1.memorySegment, p2.memorySegment))
                            .thenComparing(p -> p.index.number(), Comparator.reverseOrder())
            );

            {
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

                try {
                    int totalSStables = getTotalSStables();
                    for (int i = totalSStables; i > 0; i--) {
                        long offset = findOffsetInIndex(from, to, i);
                        if (offset != -1) {
                            priorityQueue.add(new Pair(
                                    getKeyAtIndexOffset(offset, i),
                                    new Index(i, offset, indexBaseName, sstableBaseName, storagePath, arena)
                            ));
                        }
                    }
                    if (!subMap.isEmpty()) {
                        priorityQueue.add(new Pair(subMap.firstEntry().getKey(), new MemTable(subMap)));
                    }
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
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

                Pair minPair = priorityQueue.remove();
                MemorySegment key = minPair.memorySegment();
                Table index = minPair.index();
                MemorySegment value = index.getValueFromStorage();
                removeExpiredValues(key);

                MemorySegment minPairNextKey = index.nextKey();
                if (minPairNextKey != null && (to == null || compareMemorySegments(minPairNextKey, to) < 0)) {
                    priorityQueue.add(new Pair(minPairNextKey, index));
                }
                if (value == null) return next();
                return new BaseEntry<>(key, value);
            }

            private void removeExpiredValues(MemorySegment inMemoryPriorityQueueFirstKey) {
                while (!priorityQueue.isEmpty() && priorityQueue.peek().memorySegment().mismatch(inMemoryPriorityQueueFirstKey) == -1) {
                    Table indexWithSameMin = priorityQueue.remove().index();
                    MemorySegment nextKey = indexWithSameMin.nextKey();
                    if (nextKey != null && (to == null || compareMemorySegments(nextKey, to) < 0)) {
                        priorityQueue.add(new Pair(nextKey, indexWithSameMin));
                    } else {
                        indexWithSameMin.close();
                    }
                }
            }

            private void cleanUpSStableQueue() {
                if (!priorityQueue.isEmpty()) {
                    Pair minPair = priorityQueue.element();
                    MemorySegment key = minPair.memorySegment();
                    Table index = minPair.index();
                    MemorySegment value = index.getValueFromStorage();

                    if (value == null) {
                        priorityQueue.remove();
                        removeExpiredValues(key);
                        MemorySegment minPairNextKey = index.nextKey();
                        if (minPairNextKey != null && (to == null || compareMemorySegments(minPairNextKey, to) < 0)) {
                            priorityQueue.add(new Pair(minPairNextKey, index));
                        }
                        cleanUpSStableQueue();
                    }
                }
            }
        };
    }

    private MemorySegment getKeyAtIndexOffset(long offset, int indexNum) throws IOException {
        Path filePath = storagePath.resolve(indexBaseName + indexNum);
        try (FileChannel fc = FileChannel.open(filePath, StandardOpenOption.READ)) {
            offset += Long.BYTES;
            MemorySegment mapped = fc.map(FileChannel.MapMode.READ_ONLY, 0, Files.size(filePath), arena);
            long msSize = mapped.get(ValueLayout.JAVA_LONG_UNALIGNED, offset);
            offset += Long.BYTES;
            return mapped.asSlice(offset, msSize);
        }

    }


    // Finds offset in index file to key such
    // that it is greater or equal to from
    private long findOffsetInIndex(MemorySegment from, MemorySegment to, int i) throws IOException {
        Path indexFilePath = storagePath.resolve(indexBaseName + i);
        Path indexIndexFilePath = storagePath.resolve(indexAboveIndexBaseName + i);

        try (FileChannel indexFileChannel = FileChannel.open(indexFilePath, StandardOpenOption.READ);
             FileChannel indexIndexFileChannel = FileChannel.open(indexIndexFilePath, StandardOpenOption.READ)
        ) {
            long readOffset = 0;
            MemorySegment indexMapped = indexFileChannel.map(FileChannel.MapMode.READ_ONLY, 0, Files.size(indexFilePath), arena);
            MemorySegment indexIndexMapped = indexIndexFileChannel.map(FileChannel.MapMode.READ_ONLY, 0, Files.size(indexIndexFilePath), arena);

            if (from != null) {
                FoundSegmentIndexIndexValue found = upperBound(from, indexMapped, indexIndexMapped, indexIndexFilePath);
                MemorySegment foundMemorySegment = found.found();
                if (compareMemorySegments(foundMemorySegment, from) < 0 || to != null && compareMemorySegments(foundMemorySegment, to) >= 0) {
                    return -1;
                }
                return indexIndexMapped.get(ValueLayout.JAVA_LONG_UNALIGNED, found.index() * 2 * Long.BYTES + Long.BYTES);
            } else {
                long firstKeySize = indexMapped.get(ValueLayout.JAVA_LONG_UNALIGNED, readOffset);
                readOffset += Long.BYTES;
                readOffset += firstKeySize;

                if (to == null) {
                    readOffset += indexMapped.get(ValueLayout.JAVA_LONG_UNALIGNED, readOffset);
                    readOffset += Long.BYTES;

                    return readOffset;
                } // from == null && to != null

                MemorySegment firstKey = indexMapped.asSlice(readOffset - firstKeySize, firstKeySize);
                readOffset += indexMapped.get(ValueLayout.JAVA_LONG_UNALIGNED, readOffset);
                readOffset += Long.BYTES;

                if (compareMemorySegments(firstKey, to) >= 0) {
                    return -1;
                }
                return readOffset;
            }
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

        try {
            int totalSStables = getTotalSStables();
            for (int sstableNum = totalSStables; sstableNum > 0; sstableNum--) {
//                Path currSStablePath = storagePath.resolve(sstableBaseName + sstableNum);
                var foundEntry = seekForValueInFile(key, sstableNum);
                if (foundEntry != null) {
                    if (foundEntry.value() != null) {
                        return foundEntry;
                    }
                    return null;
                }
            }
        } catch (IOException e) {
//            throw new UncheckedIOException(e);
        }
        return null;
    }

    private Entry<MemorySegment> seekForValueInFile(MemorySegment key, int sstableNum) {
        Path storageFilePath = storagePath.resolve(sstableBaseName + sstableNum);
        Path indexFilePath = storagePath.resolve(indexBaseName + sstableNum);
        Path indexIndexFilePath = storagePath.resolve(indexAboveIndexBaseName + sstableNum);

        if (!Files.exists(storageFilePath)) {
            return null;
        }

        try (FileChannel storageFileChannel = FileChannel.open(storageFilePath, StandardOpenOption.READ);
             FileChannel indexFileChannel = FileChannel.open(indexFilePath, StandardOpenOption.READ);
             FileChannel indexIndexFileChannel = FileChannel.open(indexIndexFilePath, StandardOpenOption.READ)
        ) {
            MemorySegment storageMapped = storageFileChannel.map(FileChannel.MapMode.READ_ONLY, 0, Files.size(storageFilePath), arena);
            MemorySegment indexMapped = indexFileChannel.map(FileChannel.MapMode.READ_ONLY, 0, Files.size(indexFilePath), arena);
            MemorySegment indexIndexMapped = indexIndexFileChannel.map(FileChannel.MapMode.READ_ONLY, 0, Files.size(indexIndexFilePath), arena);

            FoundSegmentIndexIndexValue found = upperBound(key, indexMapped, indexIndexMapped, indexIndexFilePath);
            if (compareMemorySegments(found.found(), key) != 0) {
                return null;
            } else {
                return getEntryFromIndexIndexFile(storageMapped, indexMapped, indexIndexMapped, found.index());
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private FoundSegmentIndexIndexValue upperBound(MemorySegment key, MemorySegment indexMapped, MemorySegment indexIndexMapped, Path indexIndexFilePath) throws IOException {
        long l = -1;
        long r = indexIndexMapped.get(ValueLayout.JAVA_LONG_UNALIGNED, Files.size(indexIndexFilePath) - 2 * Long.BYTES);

        while (r - l > 1) {
            long m = (r + l) / 2;
            MemorySegment ms = getKeyFromIndexIndexFile(indexMapped, indexIndexMapped, m);

            if (compareMemorySegments(key, ms) > 0) {
                l = m;
            } else {
                r = m;
            }
        }

        MemorySegment found = getKeyFromIndexIndexFile(indexMapped, indexIndexMapped, r);
        return new FoundSegmentIndexIndexValue(found, r);
    }

    private record FoundSegmentIndexIndexValue(MemorySegment found, long index) {
    }

    private Entry<MemorySegment> getEntryFromIndexIndexFile(MemorySegment storageMapped, MemorySegment indexMapped, MemorySegment indexIndexMapped, long m) {
        long offsetInIndexFile = indexIndexMapped.get(ValueLayout.JAVA_LONG_UNALIGNED, 2 * Long.BYTES * m + Long.BYTES);

        long offsetInStorageFile = indexMapped.get(ValueLayout.JAVA_LONG_UNALIGNED, offsetInIndexFile);

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

    private MemorySegment getKeyFromIndexIndexFile(MemorySegment indexMapped, MemorySegment indexIndexMapped, long m) {
        long offsetInIndexFile = indexIndexMapped.get(ValueLayout.JAVA_LONG_UNALIGNED, 2 * Long.BYTES * m + Long.BYTES);

        long msSize = indexMapped.get(ValueLayout.JAVA_LONG_UNALIGNED, offsetInIndexFile + Long.BYTES);
        return indexMapped.asSlice(offsetInIndexFile + 2 * Long.BYTES, msSize);
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
    }

    private void writeMapIntoFile() throws IOException {
        if (map.isEmpty()) {
            return;
        }

        int currSStableNum = getTotalSStables() + 1;
        Path sstablePath = storagePath.resolve(sstableBaseName + currSStableNum);
        Path indexPath = storagePath.resolve(indexBaseName + currSStableNum);
        Path indexIndexPath = storagePath.resolve(indexAboveIndexBaseName + currSStableNum);

        long storageWriteOffset = 0;
        long indexWriteOffset = 0;
        long indexIndexWriteOffset = 0;
        try (var storageChannel = FileChannel.open(sstablePath,
                StandardOpenOption.READ,
                StandardOpenOption.WRITE,
                StandardOpenOption.CREATE);

             var indexChannel = FileChannel.open(indexPath,
                     StandardOpenOption.READ,
                     StandardOpenOption.WRITE,
                     StandardOpenOption.CREATE);

             var indexIndexChannel = FileChannel.open(indexIndexPath,
                     StandardOpenOption.READ,
                     StandardOpenOption.WRITE,
                     StandardOpenOption.CREATE);

             var storageArena = Arena.ofConfined();
             var indexArena = Arena.ofConfined();
             var indexIndexArena = Arena.ofConfined()) {

            MemorySegment mappedStorage = storageChannel.map(FileChannel.MapMode.READ_WRITE, 0, calcMapByteSizeInFile(), storageArena);
            MemorySegment mappedIndex = indexChannel.map(FileChannel.MapMode.READ_WRITE, 0, calcIndexByteSizeInFile(), indexArena);
            MemorySegment mappedIndexIndex = indexIndexChannel.map(FileChannel.MapMode.READ_WRITE, 0, calcIndexIndexByteSizeInFile(), indexIndexArena);

            var firstKey = map.firstKey();
            writeMemorySegment(firstKey, mappedIndex, indexWriteOffset);
            indexWriteOffset += Long.BYTES;
            indexWriteOffset += firstKey.byteSize();

            var lastKey = map.lastKey();
            writeMemorySegment(lastKey, mappedIndex, indexWriteOffset);
            indexWriteOffset += Long.BYTES;
            indexWriteOffset += lastKey.byteSize();

            long entryNum = 0;
            for (var entry : map.values()) {
                writeMemorySegment(entry.key(), mappedStorage, storageWriteOffset);

                mappedIndex.set(ValueLayout.JAVA_LONG_UNALIGNED, indexWriteOffset, storageWriteOffset);
                indexWriteOffset += Long.BYTES;
                writeMemorySegment(entry.key(), mappedIndex, indexWriteOffset);
                indexWriteOffset += Long.BYTES;
                indexWriteOffset += entry.key().byteSize();

                mappedIndexIndex.set(ValueLayout.JAVA_LONG_UNALIGNED, indexIndexWriteOffset, entryNum);
                indexIndexWriteOffset += Long.BYTES;
                mappedIndexIndex.set(ValueLayout.JAVA_LONG_UNALIGNED, indexIndexWriteOffset, indexWriteOffset - entry.key().byteSize() - 2 * Long.BYTES);
                entryNum++;

                indexIndexWriteOffset += Long.BYTES;

                storageWriteOffset += Long.BYTES;
                storageWriteOffset += entry.key().byteSize();

                writeMemorySegment(entry.value(), mappedStorage, storageWriteOffset);
                storageWriteOffset += Long.BYTES;
                if (entry.value() != null) storageWriteOffset += entry.value().byteSize();
            }
            mappedStorage.load();
        }
    }

    private long calcIndexIndexByteSizeInFile() {
        return map.size() * 2L * Long.BYTES;
    }


    private int getTotalSStables() throws IOException {
        return Integer.parseInt(Files.readString(metaFilePath));
    }

    private long calcIndexByteSizeInFile() {
        return map.keySet().stream().mapToLong(k -> k.byteSize() + 2 * Long.BYTES).sum() + map.firstKey().byteSize() + map.lastKey().byteSize() + 2 * Long.BYTES;
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
