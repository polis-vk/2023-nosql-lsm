package ru.vk.itmo.volkovnikita;

import ru.vk.itmo.Entry;

import java.lang.foreign.MemorySegment;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Queue;

public class MergeIterator implements Iterator<Entry<MemorySegment>> {
    private final Queue<IndexIterator> priorityQueue;

    public MergeIterator(List<IndexIterator> iterators) {
        if (iterators.isEmpty()) {
            priorityQueue = new PriorityQueue<>();
            return;
        }
        priorityQueue = new PriorityQueue<>(
                iterators.size(),
                Comparator.comparing(
                                (IndexIterator iter) -> iter.peek().key(),
                                MemorySegmentComparator::compare)
                        .thenComparing(IndexIterator::order)
        );

        for (IndexIterator iterator : iterators) {
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
            IndexIterator currentItr = priorityQueue.remove();
            Entry<MemorySegment> current = currentItr.next();
            deleteOldEntity(current.key());
            if (currentItr.hasNext()) {
                priorityQueue.add(currentItr);
            }
        }
    }

    @Override
    public Entry<MemorySegment> next() {
        IndexIterator iterator = priorityQueue.remove();
        Entry<MemorySegment> next = iterator.next();
        deleteOldEntity(next.key());
        if (iterator.hasNext()) {
            priorityQueue.add(iterator);
        }
        return next;
    }

    private void deleteOldEntity(MemorySegment key) {
        while (!priorityQueue.isEmpty()) {
            IndexIterator iter = priorityQueue.peek();
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
