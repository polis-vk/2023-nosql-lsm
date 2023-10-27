package ru.vk.itmo.tyapuevdmitrij;

import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.PriorityQueue;

public class MergeIterator<T> implements Iterator<T> {

    private final PriorityQueue<PeekIterator<T>> priorityQueue;
    private final Comparator<T> comparator;
    PeekIterator<T> tableIterator;

    private static class PeekIterator<T> implements Iterator<T> {

        public final int id;
        private final Iterator<T> delegate;
        private T memorySegmentsEntry;

        private PeekIterator(int id, Iterator<T> delegate) {
            this.id = id;
            this.delegate = delegate;
        }

        @Override
        public boolean hasNext() {
            if (memorySegmentsEntry == null) {
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
            this.memorySegmentsEntry = null;
            return peek;
        }

        private T peek() {
            if (memorySegmentsEntry == null) {
                if (!delegate.hasNext()) {
                    return null;
                }
                memorySegmentsEntry = delegate.next();
            }
            return memorySegmentsEntry;
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
        while (tableIterator == null) {
            tableIterator = priorityQueue.poll();
            if (tableIterator == null) {
                return null;
            }
            peekFromPriorityQueue();
            if (tableIterator.peek() == null) {
                tableIterator = null;
                continue;
            }

            if (skip(tableIterator.peek())) {
                tableIterator.next();
                if (tableIterator.hasNext()) {
                    priorityQueue.add(tableIterator);
                }
                tableIterator = null;
            }
        }

        return tableIterator;
    }

    private void peekFromPriorityQueue() {
        while (true) {
            PeekIterator<T> next = priorityQueue.peek();
            if (next == null) {
                break;
            }
            int compare = comparator.compare(tableIterator.peek(), next.peek());
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
        return false;
    }

    @Override
    public boolean hasNext() {
        return peek() != null;
    }

    @Override
    public T next() {
        PeekIterator<T> entryIterator = peek();
        if (entryIterator == null) {
            throw new NoSuchElementException();
        }
        T next = entryIterator.next();
        this.tableIterator = null;
        if (entryIterator.hasNext()) {
            priorityQueue.add(entryIterator);
        }
        return next;
    }
}
