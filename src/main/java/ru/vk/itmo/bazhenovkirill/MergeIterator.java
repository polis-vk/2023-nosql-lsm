package ru.vk.itmo.bazhenovkirill;

import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.PriorityQueue;

public class MergeIterator<T> implements Iterator<T> {

    private final PriorityQueue<PeekIterator<T>> priorityQueue;

    private final Comparator<T> comparator;

    private PeekIterator<T> peek;

    private static class PeekIterator<T> implements Iterator<T> {

        private final Iterator<T> iterator;
        private T next;
        private final int id;

        public PeekIterator(int id, Iterator<T> iterator) {
            this.id = id;
            this.iterator = iterator;
            if (iterator.hasNext()) {
                next = iterator.next();
            }
        }

        private T peek() {
            return next;
        }

        @Override
        public boolean hasNext() {
            return next != null || iterator.hasNext();
        }

        @Override
        public T next() {
            T curr = next;
            next = iterator.hasNext() ? iterator.next() : null;
            return curr;
        }
    }

    public MergeIterator(Collection<Iterator<T>> iterators, Comparator<T> comparator) {
        this.comparator = comparator;
        Comparator<PeekIterator<T>> peekComp = (o1, o2) -> comparator.compare(o1.peek(), o2.peek());
        priorityQueue = new PriorityQueue<>(
                iterators.size(),
                peekComp.thenComparing(o -> -o.id)
        );

        int id = 0;
        for (Iterator<T> iterator : iterators) {
            if (iterator.hasNext()) {
                priorityQueue.add(new PeekIterator<>(id++, iterator));
            }
        }
    }

    protected boolean skip(T t) {
        return false;
    }

    private PeekIterator<T> peekIterator() {
        while (peek == null) {
            peek = priorityQueue.poll();
            if (peek == null) {
                return null;
            }

            while (true) {
                PeekIterator<T> next = priorityQueue.peek();
                if (next == null) {
                    break;
                }

                int compare = comparator.compare(peek.peek(), next.peek());
                if (compare == 0) {
                    PeekIterator<T> poll = priorityQueue.poll();
                    if (poll != null) {
                        poll.next();
                        if (poll.hasNext()) {
                            priorityQueue.add(poll);
                        }
                    }
                } else {
                    break;
                }
            }

            if (peek.peek() == null) {
                peek = null;
                continue;
            }

            if (skip(peek.peek())) {
                peek.next();
                if (peek.hasNext()) {
                    priorityQueue.add(peek);
                }
                peek = null;
            }
        }

        return peek;
    }

    @Override
    public boolean hasNext() {
        return peekIterator() != null;
    }

    @Override
    public T next() {
        PeekIterator<T> peek = peekIterator();
        if (peek == null) {
            throw new NoSuchElementException();
        }
        T next = peek.next();
        this.peek = null;
        if (peek.hasNext()) {
            priorityQueue.add(peek);
        }
        return next;
    }
}
