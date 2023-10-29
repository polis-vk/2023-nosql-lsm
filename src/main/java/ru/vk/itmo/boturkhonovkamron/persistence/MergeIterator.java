package ru.vk.itmo.boturkhonovkamron.persistence;

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

    PeekIterator<T> peekIter;

    public MergeIterator(Collection<Iterator<T>> iterators, Comparator<T> comparator) {
        this.comparator = comparator;
        Comparator<PeekIterator<T>> peekComp = (o1, o2) -> comparator.compare(o1.peek(), o2.peek());
        priorityQueue = new PriorityQueue<>(iterators.size(), peekComp.thenComparing(o -> -o.id));

        int id = 0;
        for (Iterator<T> iterator : iterators) {
            if (iterator.hasNext()) {
                priorityQueue.add(new PeekIterator<>(id++, iterator));
            }
        }
    }

    private PeekIterator<T> peek() {
        while (true) {
            if (peekIter == null) {
                peekIter = priorityQueue.poll();
                if (peekIter == null) {
                    return null;
                }
            }

            processNextItemsWithSamePriority();

            if (peekIter.peek() == null) {
                peekIter = null;
            } else if (skip(peekIter.peek())) {
                processSkippedItem();
            } else {
                break;
            }
        }

        return peekIter;
    }

    private void processNextItemsWithSamePriority() {
        while (hasNextItemWithSamePriority()) {
            handleNextItemWithSamePriority();
        }
    }

    private boolean hasNextItemWithSamePriority() {
        PeekIterator<T> next = priorityQueue.peek();
        return (next != null) && (comparator.compare(peekIter.peek(), next.peek()) == 0);
    }

    private void handleNextItemWithSamePriority() {
        PeekIterator<T> poll = priorityQueue.poll();
        if (poll != null) {
            poll.next();
            if (poll.hasNext()) {
                priorityQueue.add(poll);
            }
        }
    }

    private void processSkippedItem() {
        peekIter.next();
        if (peekIter.hasNext()) {
            priorityQueue.add(peekIter);
        } else {
            peekIter = null;
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
        PeekIterator<T> peek = peek();
        if (peek == null) {
            throw new NoSuchElementException();
        }
        T next = peek.next();
        this.peekIter = null;
        if (peek.hasNext()) {
            priorityQueue.add(peek);
        }
        return next;
    }

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
            T peek = peek();
            this.peek = null;
            return peek;
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
}
