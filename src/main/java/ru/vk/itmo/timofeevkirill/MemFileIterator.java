package ru.vk.itmo.timofeevkirill;

import ru.vk.itmo.BaseEntry;
import ru.vk.itmo.Entry;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.ArrayDeque;
import java.util.Comparator;
import java.util.Deque;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.NavigableMap;
import java.util.PriorityQueue;
import java.util.concurrent.ConcurrentSkipListMap;

class MemFileIterator implements Iterator<Entry<MemorySegment>> {
    private boolean isInit;
    private NavigableMap<MemorySegment, Entry<MemorySegment>> sortedEntries;
    private Iterator<Entry<MemorySegment>> iterator;
    private Map<MemorySegment, Long> biteCounts;
    private Map<Long, Deque<Entry<MemorySegment>>> entries;
    private Map<MemorySegment, Long> tempOffsets;

    private final Comparator<MemorySegment> comparator;
    private final Comparator<FileEntry> numberComparator;
    private final NavigableMap<Long, MemorySegment> readMappedMemorySegments;
    private final NavigableMap<MemorySegment, Entry<MemorySegment>> memTableMap;
    private final MemorySegment from;
    private final MemorySegment to;

    public MemFileIterator(Comparator<MemorySegment> comparator,
                           NavigableMap<Long, MemorySegment> readMappedMemorySegments,
                           NavigableMap<MemorySegment, Entry<MemorySegment>> memTableMap,
                           MemorySegment from, MemorySegment to) {
        this.comparator = comparator;
        this.numberComparator = new MSNumberComparator(comparator);
        this.readMappedMemorySegments = readMappedMemorySegments;
        this.memTableMap = memTableMap;
        this.from = from;
        this.to = to;
        this.isInit = false;
    }

    private void init() {
        if (isInit) {
            return;
        }
        sortedEntries = new ConcurrentSkipListMap<>(comparator);
        biteCounts = new HashMap<>();
        for (MemorySegment segment : readMappedMemorySegments.values()) {
            biteCounts.put(segment, 0L);
        }
        entries = new HashMap<>();
        for (Long number : readMappedMemorySegments.keySet()) {
            entries.put(number, new ArrayDeque<>());
        }
        if (!memTableMap.isEmpty()) {
            entries.put(Long.MAX_VALUE, new ArrayDeque<>(memTableMap.values()));
        }

        tempOffsets = new HashMap<>();
        for (MemorySegment segment : readMappedMemorySegments.values()) {
            tempOffsets.put(segment, 0L);
        }

        isInit = true;
    }

    @Override
    public boolean hasNext() {
        if (iterator != null && iterator.hasNext()) {
            return true;
        } else {
            return assignIterator();
        }
    }

    private boolean assignIterator() {
        if (readMappedMemorySegments.isEmpty()) {
            if (iterator == null) {
                iterator = new FilteredNullFTIterator(memTableMap, from, to);
                return iterator.hasNext();
            }
        } else {
            init();
            while (bite()) {
                iterator = new FilteredNullFTIterator(sortedEntries, from, to);
                if (iterator.hasNext()) return true;
            }
        }
        return false;
    }

    @Override
    public Entry<MemorySegment> next() {
        return iterator.next();
    }

    private boolean bite() {
        Iterator<Map.Entry<Long, Deque<Entry<MemorySegment>>>> iter = entries.entrySet().iterator();
        while (iter.hasNext()) {
            Map.Entry<Long, Deque<Entry<MemorySegment>>> entry = iter.next();
            Deque<Entry<MemorySegment>> value = entry.getValue();

            boolean fetched;
            if (value.isEmpty()) {
                fetched = biteFile(entry);
                if (!fetched) {
                    iter.remove();
                }
            }
        }

        if (entries.isEmpty()) {
            return false;
        } else {
            sortedEntries.clear();
            sortedEntries.putAll(mergeSortedEntries());
            return true;
        }
    }

    private boolean biteFile(Map.Entry<Long, Deque<Entry<MemorySegment>>> entry) {
        MemorySegment readMappedMemorySegment = readMappedMemorySegments.get(entry.getKey());
        if (readMappedMemorySegment == null) {
            return false;
        }

        Long offset = tempOffsets.get(readMappedMemorySegment);
        Long biteCount = biteCounts.get(readMappedMemorySegment);
        while (offset < readMappedMemorySegment.byteSize()) {
            biteCount++;

            while (offset < Constants.PAGE_SIZE * biteCount && offset < readMappedMemorySegment.byteSize()) {
                long keySize = readMappedMemorySegment.get(ValueLayout.JAVA_LONG_UNALIGNED, offset);
                offset += Long.BYTES;
                offset += keySize;
                long valueSize = readMappedMemorySegment.get(ValueLayout.JAVA_LONG_UNALIGNED, offset);
                offset += Long.BYTES;
                MemorySegment keyMemorySegment = readMappedMemorySegment
                        .asSlice(offset - keySize - Long.BYTES, keySize);
                MemorySegment valueMemorySegment;
                if (valueSize == -1L) {
                    valueMemorySegment = null;
                } else {
                    valueMemorySegment = readMappedMemorySegment.asSlice(offset, valueSize);
                    offset += valueSize;
                }

                entry.getValue().add(new BaseEntry<>(keyMemorySegment, valueMemorySegment));
            }
        }
        tempOffsets.put(readMappedMemorySegment, offset);
        biteCounts.put(readMappedMemorySegment, biteCount);

        return !entry.getValue().isEmpty();
    }

    private NavigableMap<MemorySegment, BaseEntry<MemorySegment>> mergeSortedEntries() {
        NavigableMap<MemorySegment, BaseEntry<MemorySegment>> result = new ConcurrentSkipListMap<>(comparator);
        PriorityQueue<FileEntry> priorityQueue = new PriorityQueue<>(numberComparator);
        initPriorityQueue(priorityQueue);

        while (!priorityQueue.isEmpty()) {
            FileEntry fileEntry = priorityQueue.poll();
            MemorySegment fileEntryKey = fileEntry.entry().key();
            MemorySegment fileEntryValue = fileEntry.entry().value();
            if (fileEntryValue != null) {
                result.put(fileEntryKey, new BaseEntry<>(fileEntryKey, fileEntryValue));
            }
            Deque<Entry<MemorySegment>> list = entries.get(fileEntry.number());
            list.removeFirst();

            boolean isOtherEmpty = clearOtherLists(priorityQueue, fileEntryKey);
            if (list.isEmpty() || isOtherEmpty) {
                break;
            }

            Entry<MemorySegment> nextEntry = list.getFirst();
            priorityQueue.add(new FileEntry(nextEntry, fileEntry.number()));
        }

        return result;
    }

    private boolean clearOtherLists(PriorityQueue<FileEntry> priorityQueue, MemorySegment fileEntryKey) {
        boolean isOtherEmpty = false;
        while (!priorityQueue.isEmpty() && priorityQueue.peek().entry().key().mismatch(fileEntryKey) == -1) {
            FileEntry nextFileEntry = priorityQueue.poll();
            long otherNumber = nextFileEntry.number();
            Deque<Entry<MemorySegment>> otherList = entries.get(otherNumber);
            otherList.removeFirst();
            if (otherList.isEmpty()) {
                isOtherEmpty = true;
            } else {
                Entry<MemorySegment> next = otherList.getFirst();
                priorityQueue.add(new FileEntry(next, nextFileEntry.number()));
            }
        }
        return isOtherEmpty;
    }

    private void initPriorityQueue(PriorityQueue<FileEntry> priorityQueue) {
        for (long number : entries.keySet()) {
            Deque<Entry<MemorySegment>> list = entries.get(number);
            if (!list.isEmpty()) {
                Entry<MemorySegment> entry = list.getFirst();
                priorityQueue.add(new FileEntry(entry, number));
            }
        }
    }

}
