package ru.vk.itmo.timofeevkirill;

import ru.vk.itmo.BaseEntry;
import ru.vk.itmo.Entry;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.*;
import java.util.concurrent.ConcurrentSkipListMap;

class MemFileIterator implements Iterator<Entry<MemorySegment>> {
    private boolean isInit = false;
    private NavigableMap<MemorySegment, Entry<MemorySegment>> sortedEntries = null;
    private Iterator<Entry<MemorySegment>> iterator;
    private Map<MemorySegment, Long> biteCounts = null;
    private Map<Long, Deque<Entry<MemorySegment>>> entries = null;
    private Map<MemorySegment, Long> tempOffsets = null;

    private final Comparator<MemorySegment> comparator;
    private final NavigableMap<Long, MemorySegment> readMappedMemorySegments;
    private final NavigableMap<MemorySegment, Entry<MemorySegment>> memTableMap;
    private final MemorySegment from;
    private final MemorySegment to;

    public MemFileIterator(Comparator<MemorySegment> comparator,
                           NavigableMap<Long, MemorySegment> readMappedMemorySegments,
                           NavigableMap<MemorySegment, Entry<MemorySegment>> memTableMap,
                           MemorySegment from, MemorySegment to) {
        this.comparator = comparator;
        this.readMappedMemorySegments = readMappedMemorySegments;
        this.memTableMap = memTableMap;
        this.from = from;
        this.to = to;
    }

    private void init() {
        if (!isInit) {
            sortedEntries = new ConcurrentSkipListMap<>(comparator);
            biteCounts = new HashMap<>();
            for (MemorySegment segment : readMappedMemorySegments.values()) {
                biteCounts.put(segment, 0L);
            }
            entries = new HashMap<>();
            for (Long number : readMappedMemorySegments.keySet()) {
                entries.put(number, new ArrayDeque<>());
            }
            if (!memTableMap.isEmpty())
                entries.put(Long.MAX_VALUE, new ArrayDeque<>(memTableMap.values()));

            tempOffsets = new HashMap<>();
            for (MemorySegment segment : readMappedMemorySegments.values()) {
                tempOffsets.put(segment, 0L);
            }

            isInit = true;
        }
    }


    @Override
    public boolean hasNext() {
        if (iterator != null && iterator.hasNext()) {
            return true;
        } else {
            if (readMappedMemorySegments.isEmpty()) {
                if (iterator == null) {
                    iterator = new FilteredNullFTIterator(memTableMap, from, to);
                    return iterator.hasNext();
                }
            } else {
                init();
                while (bite()) {
                    iterator = new FTIterator(sortedEntries, from, to);
                    if (iterator.hasNext()) return true;
                }
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
            Collection<Entry<MemorySegment>> value = entry.getValue();

            boolean fetched;
            if (value.isEmpty()) {
                fetched = biteFile(entry);
                if (!fetched)
                    iter.remove();
            }
        }

        if (!entries.isEmpty()) {
            sortedEntries.clear();
            sortedEntries.putAll(mergeSortedEntries());
            return true;
        } else
            return false;
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
                MemorySegment keyMemorySegment = readMappedMemorySegment.asSlice(offset, keySize);
                offset += keySize;
                long valueSize = readMappedMemorySegment.get(ValueLayout.JAVA_LONG_UNALIGNED, offset);
                offset += Long.BYTES;
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
        PriorityQueue<FileEntry> priorityQueue =
                new PriorityQueue<>(
                        (e1, e2) -> {
                            int compareResult = comparator.compare(e1.entry().key(), e2.entry().key());
                            if (compareResult == 0) {
                                int numberComparison = Long.compare(e2.number(), e1.number());
                                if (numberComparison != 0)
                                    return numberComparison;
                            }
                            return compareResult;
                        });

        for (long number : entries.keySet()) {
            Deque<Entry<MemorySegment>> list = entries.get(number);
            if (!list.isEmpty()) {
                Entry<MemorySegment> entry = list.getFirst();
                priorityQueue.add(new FileEntry(entry, number));
            }
        }

        loop:
        while (!priorityQueue.isEmpty()) {
            FileEntry fileEntry = priorityQueue.poll();
            if (fileEntry.entry().value() != null)
                result.put(fileEntry.entry().key(), new BaseEntry<>(fileEntry.entry().key(), fileEntry.entry().value()));
            Deque<Entry<MemorySegment>> list = entries.get(fileEntry.number());
            list.removeFirst();

            boolean isOtherEmpty = false;
            while (!priorityQueue.isEmpty() && priorityQueue.peek().entry().key().mismatch(fileEntry.entry().key()) == -1) {
                FileEntry mNextEntry = priorityQueue.poll();
                long otherNumber = mNextEntry.number();
                Deque<Entry<MemorySegment>> otherList = entries.get(otherNumber);
                otherList.removeIf(entry -> entry.key().mismatch(mNextEntry.entry().key()) == -1);
                if (otherList.isEmpty()) {
                    isOtherEmpty = true;
                } else {
                    Entry<MemorySegment> next = otherList.getFirst();
                    priorityQueue.add(new FileEntry(next, mNextEntry.number()));
                }
            }

            if (list.isEmpty() || isOtherEmpty) {
                break;
            }

            Entry<MemorySegment> nextEntry = list.getFirst();
            while (nextEntry.key().mismatch(fileEntry.entry().key()) == -1) {
                list.removeFirst();
                if (list.isEmpty()) {
                    break loop;
                }
                nextEntry = list.getFirst();
            }
            priorityQueue.add(new FileEntry(nextEntry, fileEntry.number()));
        }

        return result;
    }

};