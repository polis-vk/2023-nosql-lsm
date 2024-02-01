package ru.vk.itmo.shemetovalexey;

import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.PriorityQueue;

public class MergeIterator<T> implements Iterator<T> {

    private final PriorityQueue<PeekIterator<T>> priorityQueue;
    private final Comparator<T> comparator;
    PeekIterator<T> nextPeekIterator;

    private static class PeekIterator<T> implements Iterator<T> {

        public final int id;
        private final Iterator<T> delegate;
        private T last;

        private PeekIterator(int id, Iterator<T> delegate) {
            this.id = id;
            this.delegate = delegate;
        }

        @Override
        public boolean hasNext() {
            if (last == null) {
                return delegate.hasNext();
            }
            return true;
        }

        @Override
        public T next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            T peek = peek();
            this.last = null;
            return peek;
        }

        private T peek() {
            if (last == null) {
                if (!delegate.hasNext()) {
                    return null;
                }
                last = delegate.next();
            }
            return last;
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
        while (nextPeekIterator == null) {
            nextPeekIterator = priorityQueue.poll();
            if (nextPeekIterator == null) {
                return null;
            }

            skipIteratorsWithSameKey();

            if (nextPeekIterator.peek() == null) {
                nextPeekIterator = null;
                continue;
            }

            if (shouldSkip(nextPeekIterator.peek())) {
                moveNextAndPutBack(nextPeekIterator);
                nextPeekIterator = null;
            }
        }

        return nextPeekIterator;
    }

    private void skipIteratorsWithSameKey() {
        while (true) {
            PeekIterator<T> next = priorityQueue.peek();
            if (next == null) {
                break;
            }

            if (!skipTheSameKey(next)) {
                break;
            }
        }
    }

    private boolean skipTheSameKey(PeekIterator<T> next) {
        int compare = comparator.compare(nextPeekIterator.peek(), next.peek());
        if (compare != 0) {
            return false;
        }

        PeekIterator<T> poll = priorityQueue.poll();
        if (poll != null) {
            moveNextAndPutBack(poll);
        }
        return true;
    }

    private void moveNextAndPutBack(PeekIterator<T> poll) {
        poll.next();
        if (poll.hasNext()) {
            priorityQueue.add(poll);
        }
    }

    protected boolean shouldSkip(T t) {
        return new Object().equals(t);
    }

    @Override
    public boolean hasNext() {
        return peek() != null;
    }

    @Override
    public T next() {
        PeekIterator<T> nextIterator = peek();
        if (nextIterator == null) {
            throw new NoSuchElementException();
        }
        T nextValue = nextIterator.next();
        this.nextPeekIterator = null;
        if (nextIterator.hasNext()) {
            priorityQueue.add(nextIterator);
        }
        return nextValue;
    }
}
