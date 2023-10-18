package ru.vk.itmo.bazhenovkirill;

import ru.vk.itmo.Entry;

import java.lang.foreign.MemorySegment;
import java.util.Iterator;
import java.util.List;
import java.util.PriorityQueue;

public class MergeIterator implements Iterator<Entry<MemorySegment>> {
    private final PriorityQueue<SSTablePeekableIterator> priorityQueue;

    private final MemTablePeekableIterator memTableIterator;

    private final MemorySegmentComparator cmp = MemorySegmentComparator.getInstance();

    private Entry<MemorySegment> current;

    public MergeIterator(MemTablePeekableIterator memTableIterator, List<SSTablePeekableIterator> iterators) {
        this.memTableIterator = memTableIterator;
        priorityQueue = new PriorityQueue<>(new SSTableIteratorComparator());
        for (var itr : iterators) {
            if (itr.hasNext()) {
                priorityQueue.offer(itr);
            }
        }

        current = getNextCurrent();
    }

    @Override
    public boolean hasNext() {
        return current != null;
    }

    @Override
    public Entry<MemorySegment> next() {
        Entry<MemorySegment> lastCurrent = current;
        current = getNextCurrent();
        return lastCurrent;
    }

    public Entry<MemorySegment> getNextCurrent() {
        if (priorityQueue.isEmpty()) {
            while (memTableIterator.hasNext() && memTableIterator.getCurrent().value() == null) {
                memTableIterator.next();
            }
            return memTableIterator.hasNext() ? memTableIterator.next() : null;
        }

        int compareResult = 1;
        if (memTableIterator.hasNext()) {
            compareResult = cmp.compare(memTableIterator.getCurrentKey(), priorityQueue.peek().getCurrentKey());
        }
        if (compareResult == 0) {
            Entry<MemorySegment> entry = memTableIterator.next();
            SSTablePeekableIterator pqIterator = priorityQueue.poll();

            while (pqIterator != null && pqIterator.getCurrentKey().mismatch(entry.key()) == -1) {
                pqIterator.next();
                if (pqIterator.hasNext()) {
                    priorityQueue.offer(pqIterator);
                }
                pqIterator = priorityQueue.poll();
            }

            if (pqIterator == null || entry.value() == null) {
                return getNextCurrent();
            }
            return entry;
        } else if (compareResult < 0) {
            return memTableIterator.next();
        } else {
            SSTablePeekableIterator pqIterator = priorityQueue.poll();
            Entry<MemorySegment> entry = pqIterator.next();
            if (pqIterator.hasNext()) {
                priorityQueue.offer(pqIterator);
            }
            pqIterator = priorityQueue.poll();

            while (pqIterator != null && pqIterator.getCurrentKey().mismatch(entry.key()) == -1) {
                pqIterator.next();
                if (pqIterator.hasNext()) {
                    priorityQueue.offer(pqIterator);
                }
                pqIterator = priorityQueue.poll();
            }


            if (pqIterator == null || entry.value() == null) {
                return getNextCurrent();
            }
            return entry;
        }
    }
}
