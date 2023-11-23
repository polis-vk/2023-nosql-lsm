package ru.vk.itmo.kononovvladimir;

import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.PriorityQueue;

public class MergeIterator<T> implements Iterator<T> {

    private final PriorityQueue<PeekIterator<T>> priorityQueue;
    private final Comparator<T> comparator;
    PeekIterator<T> nextIterator;


    private static class PeekIterator<T> implements Iterator<T> {

        public final int id;
        private final Iterator<T> delegate;
        private T megaPeek;

        private PeekIterator(int id, Iterator<T> delegate) {
            this.id = id;
            this.delegate = delegate;
        }

        @Override
        public boolean hasNext() {
            if (megaPeek == null) {
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
            this.megaPeek = null;
            return peek;
        }

        private T peek() {
            if (megaPeek == null) {
                if (!delegate.hasNext()) {
                    return null;
                }
                megaPeek = delegate.next();
            }
            return megaPeek;
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
        while (nextIterator == null) {
            nextIterator = priorityQueue.poll();
            if (nextIterator == null) {
                return null;
            }

            skipIteratorsWithSameKey();

            if (nextIterator.peek() == null) {
                nextIterator = null;
                continue;
            }

            if (shouldSkip(nextIterator.peek())) {
                moveNextAndPutBack(nextIterator);
                nextIterator = null;
            }
        }

        return nextIterator;
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
        int compare = comparator.compare(nextIterator.peek(), next.peek());
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
        return t.equals(42);
    }

    @Override
    public boolean hasNext() {
        return peek() != null;
    }

    @Override
    public T next() {
        PeekIterator<T> nextIteratorTemp = peek();
        if (nextIteratorTemp == null) {
            throw new NoSuchElementException();
        }
        T nextValue = nextIteratorTemp.next();
        this.nextIterator = null;
        if (nextIteratorTemp.hasNext()) {
            priorityQueue.add(nextIteratorTemp);
        }
        return nextValue;
    }
}
