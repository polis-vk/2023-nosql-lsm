package ru.vk.itmo.reshetnikovaleksei.iterator;

import java.lang.foreign.MemorySegment;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Queue;

import ru.vk.itmo.Entry;

public class MergeIterator implements Iterator<Entry<MemorySegment>> {
    private final Queue<PeekingIterator> queue;
    private final Comparator<MemorySegment> comparator;

    private MergeIterator(Queue<PeekingIterator> queue, Comparator<MemorySegment> comparator) {
        this.queue = queue;
        this.comparator = comparator;
    }

    public static Iterator<Entry<MemorySegment>> merge(
            List<PeekingIterator> iterators, Comparator<MemorySegment> comparator
    ) {
        if (iterators.isEmpty()) {
            return Collections.emptyIterator();
        }

        Queue<PeekingIterator> queue = new PriorityQueue<>(iterators.size(), (PeekingIterator a, PeekingIterator b) -> {
            int compare = comparator.compare(a.peek().key(), b.peek().key());

            if (compare == 0) {
                return a.priority() < b.priority() ? -1 : 1;
            }

            return compare;
        });

        iterators.removeIf(it -> !it.hasNext());
        iterators.forEach(queue::offer);

        return new MergeIterator(queue, comparator);
    }


    @Override
    public boolean hasNext() {
        return !queue.isEmpty();
    }

    @Override
    public Entry<MemorySegment> next() {
        PeekingIterator iterator = queue.remove();
        PeekingIterator poll = queue.poll();

        if (poll == null) {
            iterator.peek();
        }

        while (poll != null) {
            Entry<MemorySegment> key = iterator.peek();
            Entry<MemorySegment> findKey = poll.peek();

            if (comparator.compare(key.key(), findKey.key()) != 0) {
                break;
            }

            poll.next();
            if (poll.hasNext()) {
                queue.offer(poll);
            }

            poll = queue.poll();
        }

        if (poll != null) {
            queue.offer(poll);
        }

        Entry<MemorySegment> next = iterator.next();
        if (iterator.hasNext()) {
            queue.offer(iterator);
        }

        return next;
    }
}
