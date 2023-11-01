package ru.vk.itmo.dalbeevgeorgii;

import ru.vk.itmo.Entry;

import java.lang.foreign.MemorySegment;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Queue;

public class MergeValuesIteratorWithoutNull implements Iterator<Entry<MemorySegment>> {
    private final Queue<IndexedPeekIterator> priorityQueue;

    public MergeValuesIteratorWithoutNull(List<IndexedPeekIterator> iterators) {
        if (iterators.isEmpty()) {
            priorityQueue = new PriorityQueue<>();
            return;
        }
        priorityQueue = new PriorityQueue<>(
                iterators.size(),
                Comparator.comparing(
                                (IndexedPeekIterator iter) -> iter.peek().key(),
                                MemorySegmentComparator::compare)
                        .thenComparing(IndexedPeekIterator::order)
        );

        for (IndexedPeekIterator iterator : iterators) {
            if (iterator.hasNext()) {
                priorityQueue.add(iterator);
            }
        }
    }

    @Override
    public boolean hasNext() {
        skipDeletedEntry();
        return !priorityQueue.isEmpty();
    }

    private void skipDeletedEntry() {
        while (!priorityQueue.isEmpty() && priorityQueue.peek().peek().value() == null) {
            IndexedPeekIterator currentItr = priorityQueue.remove();
            Entry<MemorySegment> current = currentItr.next();
            deleteOldValues(current.key());
            if (currentItr.hasNext()) {
                priorityQueue.add(currentItr);
            }
        }
    }

    @Override
    public Entry<MemorySegment> next() {
        IndexedPeekIterator iterator = priorityQueue.remove();
        Entry<MemorySegment> next = iterator.next();
        deleteOldValues(next.key());
        if (iterator.hasNext()) {
            priorityQueue.add(iterator);
        }
        return next;
    }

    private void deleteOldValues(MemorySegment key) {
        while (!priorityQueue.isEmpty()) {
            IndexedPeekIterator iter = priorityQueue.peek();
            if (key.mismatch(iter.peek().key()) != -1) {
                break;
            }
            priorityQueue.remove();
            iter.next();
            if (iter.hasNext()) {
                priorityQueue.add(iter);
            }
        }
    }
}
