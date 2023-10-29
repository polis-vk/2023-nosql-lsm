package ru.vk.itmo.ershovvadim.hw4;

import ru.vk.itmo.Entry;

import java.lang.foreign.MemorySegment;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.PriorityQueue;

public class MergeIterator<T extends Entry<MemorySegment>> implements Iterator<T> {

    private final PriorityQueue<PeekIterator<T>> priorityQueue;
    private final Comparator<T> comparator;

    PeekIterator<T> peekItr;

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
            T currentPeek = peek();
            this.peek = null;
            return currentPeek;
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
        while (peekItr == null) {
            peekItr = priorityQueue.poll();
            if (peekItr == null) {
                return null;
            }

            skipEqualEntry();

            if (peekItr.peek() == null) {
                peekItr = null;
                continue;
            }

            if (skip(peekItr.peek())) {
                peekItr.next();
                if (peekItr.hasNext()) {
                    priorityQueue.add(peekItr);
                }
                peekItr = null;
            }
        }

        return peekItr;
    }

    private void skipEqualEntry() {
        while (true) {
            PeekIterator<T> next = priorityQueue.peek();
            if (next == null) {
                break;
            }

            int compare = comparator.compare(peekItr.peek(), next.peek());
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
    }

    protected boolean skip(T t) {
        return t.value() == null;
    }

    @Override
    public boolean hasNext() {
        return peek() != null;
    }

    @Override
    public T next() {
        PeekIterator<T> currentPeek = peek();
        if (currentPeek == null) {
            throw new NoSuchElementException();
        }
        T next = currentPeek.next();
        this.peekItr = null;
        if (currentPeek.hasNext()) {
            priorityQueue.add(currentPeek);
        }
        return next;
    }
}
