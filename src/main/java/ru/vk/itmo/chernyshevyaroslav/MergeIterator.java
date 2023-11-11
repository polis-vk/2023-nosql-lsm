package ru.vk.itmo.chernyshevyaroslav;

import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.PriorityQueue;

public abstract class MergeIterator<T> implements Iterator<T> {

    private final PriorityQueue<PeekIterator<T>> priorityQueue;
    private final Comparator<T> comparator;
    private PeekIterator<T> peek;

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
            T newNext = peek();
            this.peek = null;
            return newNext;
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

    protected MergeIterator(Collection<Iterator<T>> iterators, Comparator<T> comparator) {
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

            PeekIterator<T> next = priorityQueue.peek();
            skipIdentical(next);

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

    private void skipIdentical(PeekIterator<T> next) {
        PeekIterator<T> result = next;
        while (result != null) {

            int compare = comparator.compare(peek.peek(), result.peek());
            PeekIterator<T> poll = priorityQueue.peek();
            if ((compare != 0) || (poll == null)) {
                break;
            }
            priorityQueue.remove();
            poll.next();
            if (poll.hasNext()) {
                priorityQueue.add(poll);
            }
            result = priorityQueue.peek();
        }
    }

    protected abstract boolean skip(T t);

    @Override
    public boolean hasNext() {
        return peek() != null;
    }

    @Override
    public T next() {
        PeekIterator<T> peekIterator = peek();
        if (peekIterator == null) {
            throw new NoSuchElementException();
        }
        T next = peekIterator.next();
        this.peek = null;
        if (peekIterator.hasNext()) {
            priorityQueue.add(peekIterator);
        }
        return next;
    }
}
