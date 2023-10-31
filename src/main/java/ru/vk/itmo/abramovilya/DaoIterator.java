package ru.vk.itmo.abramovilya;

import ru.vk.itmo.BaseEntry;
import ru.vk.itmo.Entry;
import ru.vk.itmo.abramovilya.table.MemTable;
import ru.vk.itmo.abramovilya.table.SSTable;
import ru.vk.itmo.abramovilya.table.TableEntry;

import java.lang.foreign.MemorySegment;
import java.util.Iterator;
import java.util.NavigableMap;
import java.util.NoSuchElementException;
import java.util.PriorityQueue;

class DaoIterator implements Iterator<Entry<MemorySegment>> {
    private final PriorityQueue<TableEntry> priorityQueue = new PriorityQueue<>();
    private final MemorySegment from;
    private final MemorySegment to;
    private final Storage storage;

    DaoIterator(int totalSStables,
                MemorySegment from,
                MemorySegment to,
                Storage storage,
                NavigableMap<MemorySegment, Entry<MemorySegment>> memTable) {

        this.from = from;
        this.to = to;
        this.storage = storage;

        NavigableMap<MemorySegment, Entry<MemorySegment>> subMap = getSubMap(memTable);
        for (int i = 0; i < totalSStables; i++) {
            long offset = findOffsetInIndex(from, to, i);
            if (offset != -1) {
                priorityQueue.add(new SSTable(
                        i,
                        offset,
                        storage.mappedSStable(i),
                        storage.mappedIndex(i)
                ).currentEntry());
            }
        }
        if (!subMap.isEmpty()) {
            priorityQueue.add(new MemTable(subMap).currentEntry());
        }
        cleanUpSStableQueue();
    }

    private NavigableMap<MemorySegment, Entry<MemorySegment>> getSubMap(
            NavigableMap<MemorySegment, Entry<MemorySegment>> map) {
        NavigableMap<MemorySegment, Entry<MemorySegment>> subMap;
        if (from == null && to == null) {
            subMap = map;
        } else if (from == null) {
            subMap = map.headMap(to, false);
        } else if (to == null) {
            subMap = map.tailMap(from, true);
        } else {
            subMap = map.subMap(from, true, to, false);
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
        removeExpiredValues(key);

        TableEntry minEntryTableNextEntry = minEntry.table().nextEntry();
        if (minEntryTableNextEntry != null
                && (to == null || DaoImpl.compareMemorySegments(minEntryTableNextEntry.getKey(), to) < 0)) {
            priorityQueue.add(minEntryTableNextEntry);
        }
        MemorySegment value = minEntry.getValue();
        cleanUpSStableQueue();
        return new BaseEntry<>(key, value);
    }

    private void removeExpiredValues(MemorySegment minMemorySegment) {
        while (!priorityQueue.isEmpty() && priorityQueue.peek().getKey().mismatch(minMemorySegment) == -1) {
            TableEntry entryWithSameMin = priorityQueue.remove();

            TableEntry entryWithSameMinNext = entryWithSameMin.table().nextEntry();
            if (entryWithSameMinNext != null
                    && (to == null || DaoImpl.compareMemorySegments(entryWithSameMinNext.getKey(), to) < 0)) {
                priorityQueue.add(entryWithSameMinNext);
            }
        }
    }

    private void cleanUpSStableQueue() {
        while (!priorityQueue.isEmpty() && priorityQueue.peek().getValue() == null) {
            TableEntry minEntry = priorityQueue.remove();
            removeExpiredValues(minEntry.getKey());

            TableEntry minEntryNext = minEntry.table().nextEntry();
            if (minEntryNext != null
                    && (to == null || DaoImpl.compareMemorySegments(minEntryNext.getKey(), to) < 0)) {
                priorityQueue.add(minEntryNext);
            }
        }
    }

    private long findOffsetInIndex(MemorySegment from, MemorySegment to, int fileNum) {
        return storage.findOffsetInIndex(from, to, fileNum);
    }
}
