package ru.vk.itmo.dyagayalexandra;

import ru.vk.itmo.Entry;

import java.lang.foreign.MemorySegment;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.PriorityQueue;

public final class MergedIterator implements Iterator<Entry<MemorySegment>> {

    private final PriorityQueue<PeekingIterator> iterators;
    private final Comparator<Entry<MemorySegment>> comparator;

    private MergedIterator(PriorityQueue<PeekingIterator> iterators, Comparator<Entry<MemorySegment>> comparator) {
        this.iterators = iterators;
        this.comparator = comparator;
    }

    public static Iterator<Entry<MemorySegment>> createMergedIterator(List<PeekingIterator> iterators,
                                                                      Comparator<Entry<MemorySegment>> comparator) {

        if (iterators.isEmpty()) {
            return Collections.emptyIterator();
        }

        if (iterators.size() == 1) {
            return iterators.get(0);
        }

        PriorityQueue<PeekingIterator> queue = new PriorityQueue<>(iterators.size(), (iterator1, iterator2) -> {
            int result = comparator.compare(iterator1.peek(), iterator2.peek());
            if (result != 0) {
                return result;
            }

            return Integer.compare(iterator1.getIndex(), iterator2.getIndex());
        });

        for (PeekingIterator iterator : iterators) {
            if (iterator.hasNext()) {
                queue.add(iterator);
            }
        }

        return new MergedIterator(queue, comparator);
    }

    @Override
    public boolean hasNext() {
        return !iterators.isEmpty();
    }

    @Override
    public Entry<MemorySegment> next() {
        PeekingIterator iterator = iterators.remove();
        Entry<MemorySegment> next = iterator.next();
        while (!iterators.isEmpty()) {
            PeekingIterator candidate = iterators.peek();
            if (comparator.compare(next, candidate.peek()) != 0) {
                break;
            }

            iterators.remove();
            candidate.next();
            if (candidate.hasNext()) {
                iterators.add(candidate);
            }
        }

        if (iterator.hasNext()) {
            iterators.add(iterator);
        }

        return next;
    }
}
