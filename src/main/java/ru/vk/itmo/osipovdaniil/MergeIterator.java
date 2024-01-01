package ru.vk.itmo.osipovdaniil;

import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.PriorityQueue;

public class MergeIterator<T> implements Iterator<T> {

    private final PriorityQueue<PeekIterator<T>> priorityQueue;
    private final Comparator<T> comparator;

    PeekIterator<T> peek;

    private static class PeekIterator<T> implements Iterator<T> {

        public final int id;
        private final Iterator<T> delegate;
        private T peek;

        private PeekIterator(int id, Iterator<T> delegate) {
            this.id = id;
            this.delegate = delegate;
        }

        @Override
        public boolean hasNext() {
            if (peek == null) {
                return delegate.hasNext();
            }
            return true;
        }

        @Override
        public T next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            T pk = peek();
            this.peek = null;
            return pk;
        }

        private T peek() {
            if (peek == null) {
                if (!delegate.hasNext()) {
                    return null;
                }
                peek = delegate.next();
            }
            return peek;
        }
    }

    public MergeIterator(final Collection<Iterator<T>> iterators, final Comparator<T> comparator) {
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

    private PeekIterator<T> peek() {
        while (peek == null) {
            peek = priorityQueue.poll();
            if (peek == null) {
                return null;
            }
            fillingQueue();
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

    private void fillingQueue() {
        while (true) {
            final PeekIterator<T> next = priorityQueue.peek();
            if (next == null) {
                break;
            }
            int compare = comparator.compare(peek.peek(), next.peek());
            if (compare != 0) {
                break;
            }
            PeekIterator<T> poll = priorityQueue.poll();
            if (poll == null) {
                continue;
            }
            poll.next();
            if (poll.hasNext()) {
                priorityQueue.add(poll);
            }
        }
    }

    protected boolean skip(final T t) {
        return t == null;
    }

    @Override
    public boolean hasNext() {
        return peek() != null;
    }

    @Override
    public T next() {
        final PeekIterator<T> pk = peek();
        if (pk == null) {
            throw new NoSuchElementException();
        }
        final T next = pk.next();
        this.peek = null;
        if (pk.hasNext()) {
            priorityQueue.add(pk);
        }
        return next;
    }
}
