package ru.vk.itmo.abramovilya;

import ru.vk.itmo.BaseEntry;
import ru.vk.itmo.Entry;
import ru.vk.itmo.abramovilya.table.MemTable;
import ru.vk.itmo.abramovilya.table.SSTable;
import ru.vk.itmo.abramovilya.table.TableEntry;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.PriorityQueue;
import java.util.concurrent.ConcurrentNavigableMap;

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
        cleanUpSStableQueue();
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
        return new BaseEntry<>(key, value);
    }

    private void removeExpiredValues(MemorySegment minMemorySegment) {
        while (!priorityQueue.isEmpty() && priorityQueue.peek().getKey().mismatch(minMemorySegment) == -1) {
            TableEntry entryWithSameMin = priorityQueue.remove();
            TableEntry entryWithSameMinNextEntry = entryWithSameMin.table().nextEntry();
            if (entryWithSameMinNextEntry != null &&
                    (to == null || DaoImpl.compareMemorySegments(entryWithSameMinNextEntry.getKey(), to) < 0)) {
                priorityQueue.add(entryWithSameMinNextEntry);
            }
        }
    }

    private void cleanUpSStableQueue() {
        if (!priorityQueue.isEmpty()) {
            TableEntry minEntry = priorityQueue.element();
            MemorySegment key = minEntry.getKey();
            MemorySegment value = minEntry.getValue();

            if (value == null) {
                priorityQueue.remove();
                removeExpiredValues(key);

                TableEntry minEntryNextKey = minEntry.table().nextEntry();
                if (minEntryNextKey != null &&
                        (to == null || DaoImpl.compareMemorySegments(minEntryNextKey.getKey(), to) < 0)) {
                    priorityQueue.add(minEntryNextKey);
                }
                cleanUpSStableQueue();
            }
        }
    }

    private long findOffsetInIndex(MemorySegment from, MemorySegment to, int i) {
        long readOffset = 0;
        MemorySegment storageMapped = sstableMappedList.get(i);
        MemorySegment indexMapped = indexMappedList.get(i);

        if (from != null) {
            DaoImpl.FoundSegmentIndexIndexValue found = DaoImpl.upperBound(from, storageMapped, indexMapped, indexMapped.byteSize());
            MemorySegment foundMemorySegment = found.found();
            if (DaoImpl.compareMemorySegments(foundMemorySegment, from) < 0 || (to != null && DaoImpl.compareMemorySegments(foundMemorySegment, to) >= 0)) {
                return -1;
            }
            return found.index() * 2 * Long.BYTES + Long.BYTES;
        } else {

            if (to == null) {
                return Long.BYTES;
            }

            long firstKeySize = storageMapped.get(ValueLayout.JAVA_LONG_UNALIGNED, readOffset);
            readOffset += Long.BYTES;
            MemorySegment firstKey = storageMapped.asSlice(readOffset, firstKeySize);

            if (DaoImpl.compareMemorySegments(firstKey, to) >= 0) {
                return -1;
            }
            return Long.BYTES;
        }
    }
}

