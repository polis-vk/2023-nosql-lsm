package ru.vk.itmo.bandurinvladislav.iterator;

import ru.vk.itmo.Entry;
import ru.vk.itmo.bandurinvladislav.comparator.MemorySegmentIteratorComparator;

import java.lang.foreign.MemorySegment;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.PriorityQueue;

public class MergeIterator implements Iterator<Entry<MemorySegment>> {
    private final PriorityQueue<MemorySegmentIterator> iterators = new PriorityQueue<>(new MemorySegmentIteratorComparator());

    public MergeIterator(List<MemorySegmentIterator> iteratorList) {
        iteratorList.forEach(iterators::offer);
    }

    @Override
    public boolean hasNext() {
        return !iterators.isEmpty();
    }

    @Override
    public Entry<MemorySegment> next() {
        if (iterators.isEmpty()) {
            throw new NoSuchElementException();
        }
        MemorySegmentIterator minIterator = iterators.poll();
        Entry<MemorySegment> next = minIterator.next();
        if (next == null) {
            throw new IllegalStateException("Unexpected null value");
        }
        updateQueue(minIterator, next);
        if (next.value() == null) {
            return next();
        }
        return next;
    }

    private void updateQueue(MemorySegmentIterator minIterator, Entry<MemorySegment> nextValue) {
        while (!iterators.isEmpty()) {
            MemorySegmentIterator candidate = iterators.peek();
            Entry<MemorySegment> peek = candidate.peek();
            if (nextValue.key().mismatch(peek.key()) != -1) {
                break;
            }
            iterators.poll();

            if (candidate.hasNext()) {
                iterators.offer(candidate);
            }
        }
        if (minIterator.hasNext()) {
            iterators.offer(minIterator);
        }
    }
}
