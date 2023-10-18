package ru.vk.itmo.abramovilya;

import ru.vk.itmo.BaseEntry;
import ru.vk.itmo.Entry;
import ru.vk.itmo.abramovilya.table.MemTable;
import ru.vk.itmo.abramovilya.table.SSTable;
import ru.vk.itmo.abramovilya.table.TableEntry;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.*;
import java.util.concurrent.ConcurrentNavigableMap;

import static ru.vk.itmo.abramovilya.DaoImpl.getKeyFromStorage;

class DaoIterator implements Iterator<Entry<MemorySegment>> {
    private final PriorityQueue<TableEntry> priorityQueue = new PriorityQueue<>();
    private final MemorySegment from;
    private final MemorySegment to;
    private final List<MemorySegment> sstableMappedList;
    private final List<MemorySegment> indexMappedList;

    DaoIterator(int totalSStables,
                MemorySegment from,
                MemorySegment to,
                List<MemorySegment> sstableMappedList,
                List<MemorySegment> indexMappedList,
                ConcurrentNavigableMap<MemorySegment, Entry<MemorySegment>> memTable) {
        this.from = from;
        this.to = to;
        this.sstableMappedList = sstableMappedList;
        this.indexMappedList = indexMappedList;

        ConcurrentNavigableMap<MemorySegment, Entry<MemorySegment>> subMap = getSubMap(memTable);
        for (int i = 0; i < totalSStables; i++) {
            long offset = findOffsetInIndex(from, to, i);
            if (offset != -1) {
                priorityQueue.add(new SSTable(i, offset, sstableMappedList.get(i), indexMappedList.get(i)).currentEntry());
            }
        }
        if (!subMap.isEmpty()) {
            priorityQueue.add(new MemTable(subMap).currentEntry());
        }
        cleanUpSStableQueue();
    }

    private ConcurrentNavigableMap<MemorySegment, Entry<MemorySegment>> getSubMap(ConcurrentNavigableMap<MemorySegment, Entry<MemorySegment>> map) {
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
        return !priorityQueue.isEmpty();
    }

    @Override
    public Entry<MemorySegment> next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }

        TableEntry minEntry = priorityQueue.remove();
        MemorySegment key = minEntry.getKey();
        MemorySegment value = minEntry.getValue();
        removeExpiredValues(key);

        TableEntry minEntryTableNextEntry = minEntry.table().nextEntry();
        if (minEntryTableNextEntry != null &&
                (to == null || DaoImpl.compareMemorySegments(minEntryTableNextEntry.getKey(), to) < 0)) {
            priorityQueue.add(minEntryTableNextEntry);
        }
        cleanUpSStableQueue();
        return new BaseEntry<>(key, value);
    }

    private void removeExpiredValues(MemorySegment minMemorySegment) {
        while (!priorityQueue.isEmpty() && priorityQueue.peek().getKey().mismatch(minMemorySegment) == -1) {
            TableEntry entryWithSameMin = priorityQueue.remove();

            TableEntry entryWithSameMinNext = entryWithSameMin.table().nextEntry();
            if (entryWithSameMinNext != null &&
                    (to == null || DaoImpl.compareMemorySegments(entryWithSameMinNext.getKey(), to) < 0)) {
                priorityQueue.add(entryWithSameMinNext);
            }
        }
    }

    private void cleanUpSStableQueue() {
        while (!priorityQueue.isEmpty() && priorityQueue.peek().getValue() == null) {
            TableEntry minEntry = priorityQueue.remove();
            removeExpiredValues(minEntry.getKey());

            TableEntry minEntryNext = minEntry.table().nextEntry();
            if (minEntryNext != null &&
                    (to == null || DaoImpl.compareMemorySegments(minEntryNext.getKey(), to) < 0)) {
                priorityQueue.add(minEntryNext);
            }
        }
    }

    private long findOffsetInIndex(MemorySegment from, MemorySegment to, int i) {
        long readOffset = 0;
        MemorySegment storageMapped = sstableMappedList.get(i);
        MemorySegment indexMapped = indexMappedList.get(i);

        if (from != null) {
            int foundIndex = DaoImpl.upperBound(from, storageMapped, indexMapped, indexMapped.byteSize());
            MemorySegment foundMemorySegment = getKeyFromStorage(storageMapped, indexMapped, foundIndex);
            if (DaoImpl.compareMemorySegments(foundMemorySegment, from) < 0 || (to != null && DaoImpl.compareMemorySegments(foundMemorySegment, to) >= 0)) {
                return -1;
            }
            return (long) foundIndex *  (Integer.BYTES + Long.BYTES) + Integer.BYTES;
        } else {
            if (to == null) {
                return Integer.BYTES;
            }

            long firstKeySize = storageMapped.get(ValueLayout.JAVA_LONG_UNALIGNED, readOffset);
            readOffset += Long.BYTES;
            MemorySegment firstKey = storageMapped.asSlice(readOffset, firstKeySize);

            if (DaoImpl.compareMemorySegments(firstKey, to) >= 0) {
                return -1;
            }
            return Integer.BYTES;
        }
    }
}

