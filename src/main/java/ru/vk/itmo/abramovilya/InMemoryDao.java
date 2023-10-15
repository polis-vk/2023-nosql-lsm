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

public class InMemoryDao implements Dao<MemorySegment, Entry<MemorySegment>> {
    private final ConcurrentNavigableMap<MemorySegment, Entry<MemorySegment>> map =
            new ConcurrentSkipListMap<>(InMemoryDao::compareMemorySegments);
    private final Path storagePath;
    private final Arena arena = Arena.ofShared();
    private final String sstableBaseName = "storage";
    private final String indexBaseName = "index";
    private final Path metaFilePath;


    public InMemoryDao(Config config) {
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

    record Pair(MemorySegment memorySegment, Index index) {
    }

    @Override
    public Iterator<Entry<MemorySegment>> get(MemorySegment from, MemorySegment to) {
        return new Iterator<>() {
            private final PriorityQueue<Pair> priorityQueue = new PriorityQueue<>(
                    ((Comparator<Pair>) (p1, p2) -> compareMemorySegments(p1.memorySegment, p2.memorySegment))
                            .thenComparing(p -> p.index.number, Comparator.reverseOrder())
            );

            private final PriorityQueue<Entry<MemorySegment>> inMemoryPriorityQueue = new PriorityQueue<>((o1, o2) -> compareMemorySegments(o1.key(), o2.key()));

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
                inMemoryPriorityQueue.addAll(subMap.values());

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
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            }

            @Override
            public boolean hasNext() {
                cleanupQueue();
                return !(priorityQueue.isEmpty() && inMemoryPriorityQueue.isEmpty());
            }

            @Override
            public Entry<MemorySegment> next() {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }
                if (priorityQueue.isEmpty()) {
                    if (inMemoryPriorityQueue.element().value() == null) {
                        inMemoryPriorityQueue.remove();
                        return next();
                    } else {
                        return inMemoryPriorityQueue.remove();
                    }
                }

                if (!inMemoryPriorityQueue.isEmpty()) {
                    MemorySegment inMemoryPriorityQueueFirstKey = inMemoryPriorityQueue.element().key();
                    if (compareMemorySegments(inMemoryPriorityQueueFirstKey, priorityQueue.element().memorySegment()) <= 0) {
                        removeExpiredValues(inMemoryPriorityQueueFirstKey);
                        if (inMemoryPriorityQueue.element().value() == null) {
                            inMemoryPriorityQueue.remove();
                            return next();
                        }
                        return inMemoryPriorityQueue.remove();
                    }
                }

                Pair minPair = priorityQueue.remove();
                MemorySegment key = minPair.memorySegment();
                Index index = minPair.index();
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
                    Index indexWithSameMin = priorityQueue.remove().index();
                    MemorySegment nextKey = indexWithSameMin.nextKey();
                    if (nextKey != null && (to == null || compareMemorySegments(nextKey, to) < 0)) {
                        priorityQueue.add(new Pair(nextKey, indexWithSameMin));
                    } else {
                        indexWithSameMin.close();
                    }
                }
            }

            private void cleanupQueue() {
                if (!inMemoryPriorityQueue.isEmpty() && !priorityQueue.isEmpty()) {
                    MemorySegment inMemKey = inMemoryPriorityQueue.element().key();
                    MemorySegment inFileKey = priorityQueue.element().memorySegment();
                    if (compareMemorySegments(inMemKey, inFileKey) < 0) {
                        if (inMemoryPriorityQueue.element().value() == null) {
                            inMemoryPriorityQueue.remove();
                            cleanupQueue();
                        }
                    } else if (compareMemorySegments(inMemKey, inFileKey) == 0) {
                        if (inMemoryPriorityQueue.element().value() == null) {
                            removeExpiredValues(inMemKey);
                            inMemoryPriorityQueue.remove();
                            cleanupQueue();
                        }
                    } else {
                        cleanUpSStableQueue();
                    }
                } else if (priorityQueue.isEmpty()) {
                    while (!inMemoryPriorityQueue.isEmpty() && inMemoryPriorityQueue.element().value() == null) {
                        inMemoryPriorityQueue.remove();
                    }
                } else {
                    cleanUpSStableQueue();
                }
            }

            private void cleanUpSStableQueue() {
                Pair minPair = priorityQueue.element();
                MemorySegment key = minPair.memorySegment();
                Index index = minPair.index();
                MemorySegment value = index.getValueFromStorage();

                if (value == null) {
                    priorityQueue.remove();
                    removeExpiredValues(key);
                    MemorySegment minPairNextKey = index.nextKey();
                    if (minPairNextKey != null && (to == null || compareMemorySegments(minPairNextKey, to) < 0)) {
                        priorityQueue.add(new Pair(minPairNextKey, index));
                    }
                    cleanupQueue();
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
        Path filePath = storagePath.resolve(indexBaseName + i);
        try (FileChannel fc = FileChannel.open(filePath, StandardOpenOption.READ)) {
            long readOffset = 0;
            MemorySegment mapped = fc.map(FileChannel.MapMode.READ_ONLY, 0, Files.size(filePath), arena);
            while (readOffset < Files.size(filePath)) {
//                long inStorageOffset = mapped.get(ValueLayout.JAVA_LONG_UNALIGNED, readOffset);
                readOffset += Long.BYTES;
                long msSize = mapped.get(ValueLayout.JAVA_LONG_UNALIGNED, readOffset);
                readOffset += Long.BYTES;
                MemorySegment ms = mapped.asSlice(readOffset, msSize);

                if (from == null || compareMemorySegments(ms, from) >= 0) {
                    if (to == null || compareMemorySegments(ms, to) < 0) {
                        return readOffset - 2 * Long.BYTES;
                    }
                    return -1;
                }
                readOffset += msSize;
            }
        }
        return -1;
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
                Path currSStablePath = storagePath.resolve(sstableBaseName + sstableNum);
                var foundEntry = seekForValueInFile(key, currSStablePath);
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

    private Entry<MemorySegment> seekForValueInFile(MemorySegment key, Path filePath) {
        if (!Files.exists(filePath)) {
            return null;
        }
        long readOffset = 0;
        try (FileChannel fc = FileChannel.open(filePath, StandardOpenOption.READ)) {
            MemorySegment mapped = fc.map(FileChannel.MapMode.READ_ONLY, 0, Files.size(filePath), arena);
            while (readOffset < Files.size(filePath)) {
                long size = mapped.get(ValueLayout.JAVA_LONG_UNALIGNED, readOffset);
                readOffset += Long.BYTES;
                MemorySegment keySegment = mapped.asSlice(readOffset, size);
                readOffset += size;
                if (compareMemorySegments(key, keySegment) == 0) {
                    size = mapped.get(ValueLayout.JAVA_LONG_UNALIGNED, readOffset);
                    readOffset += Long.BYTES;
                    if (size == -1) {
                        return new BaseEntry<>(key, null);
                    }
                    return new BaseEntry<>(key, mapped.asSlice(readOffset, size));
                }
            }
            return null;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
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
            for (var entry : map.values()) {
                writeMemorySegment(entry.key(), mappedStorage, storageWriteOffset);

                mappedIndex.set(ValueLayout.JAVA_LONG_UNALIGNED, indexWriteOffset, storageWriteOffset);
                indexWriteOffset += Long.BYTES;
                writeMemorySegment(entry.key(), mappedIndex, indexWriteOffset);
                indexWriteOffset += Long.BYTES;
                indexWriteOffset += entry.key().byteSize();

                storageWriteOffset += Long.BYTES;
                storageWriteOffset += entry.key().byteSize();

                writeMemorySegment(entry.value(), mappedStorage, storageWriteOffset);
                storageWriteOffset += Long.BYTES;
                if (entry.value() != null) storageWriteOffset += entry.value().byteSize();
            }
            mappedStorage.load();
        }
    }


    private int getTotalSStables() throws IOException {
        return Integer.parseInt(Files.readString(metaFilePath));
    }

    private long calcIndexByteSizeInFile() {
        return map.keySet().stream().mapToLong(k -> k.byteSize() + 2 * Long.BYTES).sum();
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
