package ru.vk.itmo.chebotinalexandr;

import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Queue;

public class MergeIterator<E> implements Iterator<E> {
    private final Queue<PeekingIterator<E>> queue;
    private final Comparator<? super E> comparator;

    public MergeIterator(Queue<PeekingIterator<E>> queue, Comparator<? super E> comparator) {
        this.queue = queue;
        this.comparator = comparator;
    }

    public static <E> Iterator<E> merge(List<PeekingIterator<E>> iterators, Comparator<? super E> comparator) {
        if (iterators.isEmpty()) {
            return Collections.emptyIterator();
        }

        Queue<PeekingIterator<E>> queue = getPeekingIterators(iterators.size(), comparator);

        iterators.removeIf(i -> !i.hasNext());
        iterators.forEach(queue::offer);

        return new MergeIterator<>(queue, comparator);
    }

    private static <E> Queue<PeekingIterator<E>> getPeekingIterators(int size, Comparator<? super E> comparator) {
        Comparator<PeekingIterator<E>> heapComparator =
                (PeekingIterator<E> o1, PeekingIterator<E> o2) -> {
                    int compare = comparator.compare(o1.peek(), o2.peek());

                    if (compare == 0) {
                        return o1.priority() < o2.priority() ? -1 : 1;
                    }

                    return compare;
                };

        return new PriorityQueue<>(size, heapComparator);
    }

    @Override
    public boolean hasNext() {
        return !queue.isEmpty();
    }

    @Override
    public E next() {
        PeekingIterator<E> iterator = queue.remove();
        PeekingIterator<E> poll = queue.poll();

        if (poll == null) {
            iterator.peek();
        }

        while (poll != null) {
            E key = iterator.peek();
            E findKey = poll.peek();
            if (comparator.compare(key, findKey) != 0) {
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

        E next = iterator.next();

        if (iterator.hasNext()) {
            queue.offer(iterator);
        }

        return next;
    }
}
