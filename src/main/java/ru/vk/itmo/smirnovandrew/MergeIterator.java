package ru.vk.itmo.smirnovandrew;

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
            T peeked = peek();
            this.peek = null;
            return peeked;
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

    private PeekIterator<T> peek() {
        while (peek == null) {
            peek = priorityQueue.poll();
            if (peek == null) {
                return null;
            }

            processConsecutiveEqualElements();

            if (peek.peek() == null) {
                peek = null;
                continue;
            }

            if (skip(peek.peek())) {
                processSkippedElement();
            }
        }

        return peek;
    }

    private void processConsecutiveEqualElements() {
        while (hasNextEqualElement()) {
            handleEqualElement();
        }
    }

    private boolean hasNextEqualElement() {
        PeekIterator<T> next = priorityQueue.peek();
        return (next != null) && (comparator.compare(peek.peek(), next.peek()) == 0);
    }

    private void handleEqualElement() {
        PeekIterator<T> poll = priorityQueue.poll();
        if (poll != null) {
            poll.next();
            if (poll.hasNext()) {
                priorityQueue.add(poll);
            }
        }
    }

    private void processSkippedElement() {
        peek.next();
        if (peek.hasNext()) {
            priorityQueue.add(peek);
        }
        peek = null;
    }

    protected boolean skip(T t) {
        return t == null;
    }

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
