package ru.vk.itmo.prokopyevnikita;

import ru.vk.itmo.Entry;

import java.lang.foreign.MemorySegment;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Queue;

public class MergeSkipNullValuesIterator implements Iterator<Entry<MemorySegment>> {
    private final Queue<OrderedPeekIterator<Entry<MemorySegment>>> priorityQueue;

    public MergeSkipNullValuesIterator(List<OrderedPeekIterator<Entry<MemorySegment>>> iterators) {
        if (iterators.isEmpty()) {
            priorityQueue = new PriorityQueue<>();
            return;
        }
        priorityQueue = new PriorityQueue<>(
                iterators.size(),
                Comparator.comparing(
                                (OrderedPeekIterator<Entry<MemorySegment>> iter) -> iter.peek().key(),
                                MemorySegmentComparator::compare)
                        .thenComparing(OrderedPeekIterator::order)
        );

        for (OrderedPeekIterator<Entry<MemorySegment>> iterator : iterators) {
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
            OrderedPeekIterator<Entry<MemorySegment>> currentItr = priorityQueue.remove();
            Entry<MemorySegment> current = currentItr.next();
            deleteOldValues(current.key());
            if (currentItr.hasNext()) {
                priorityQueue.add(currentItr);
            }
        }
    }

    @Override
    public Entry<MemorySegment> next() {
        OrderedPeekIterator<Entry<MemorySegment>> iterator = priorityQueue.remove();
        Entry<MemorySegment> next = iterator.next();
        deleteOldValues(next.key());
        if (iterator.hasNext()) {
            priorityQueue.add(iterator);
        }
        return next;
    }

    private void deleteOldValues(MemorySegment key) {
        while (!priorityQueue.isEmpty()) {
            OrderedPeekIterator<Entry<MemorySegment>> itr = priorityQueue.peek();
            if (key.mismatch(itr.peek().key()) != -1) {
                break;
            }
            priorityQueue.remove();
            itr.next();
            if (itr.hasNext()) {
                priorityQueue.add(itr);
            }
        }
    }
}
