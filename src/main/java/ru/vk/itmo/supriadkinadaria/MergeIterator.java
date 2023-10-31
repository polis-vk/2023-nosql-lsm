package ru.vk.itmo.supriadkinadaria;


import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.PriorityQueue;

public class MergeIterator<T> implements Iterator<T> {

    private final PriorityQueue<PeekIterator<T>> priorityQueue;
    private final Comparator<T> comparator;
    private PeekIterator<T> peekIterator;


    private static class PeekIterator<T> implements Iterator<T> {

        public final int id;
        private final Iterator<T> delegate;
        private T peekTmp;

        private PeekIterator(int id, Iterator<T> delegate) {
            this.id = id;
            this.delegate = delegate;
        }

        @Override
        public boolean hasNext() {
            if (peekTmp == null) {
                return delegate.hasNext();
            }
            return true;
        }

        @Override
        public T next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            T peekNext = peek();
            this.peekTmp = null;
            return peekNext;
        }

        private T peek() {
            if (peekTmp == null) {
                if (!delegate.hasNext()) {
                    return null;
                }
                peekTmp = delegate.next();
            }
            return peekTmp;
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
        while (peekIterator == null) {
            peekIterator = priorityQueue.poll();
            if (peekIterator == null) {
                return null;
            }

            PeekIterator<T> next = priorityQueue.peek();
            while (next != null) {
                int compare = comparator.compare(peekIterator.peek(), next.peek());
                if (compare == 0) {
                    PeekIterator<T> poll = priorityQueue.poll();
                    if (poll != null
                            && poll.next() != null
                            && poll.hasNext()) {
                        priorityQueue.add(poll);
                    }
                } else {
                    break;
                }
                next = priorityQueue.peek();
            }

            if (peekIterator.peek() != null
                    && skip(peekIterator.peek())
                    && peekIterator.next() != null) {
                if (peekIterator.hasNext()) {
                    priorityQueue.add(peekIterator);
                }
                peekIterator = null;
            }
        }

        return peekIterator;
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
        PeekIterator<T> peekIterator = peek();
        if (peekIterator == null) {
            throw new NoSuchElementException();
        }
        T next = peekIterator.next();
        this.peekIterator = null;
        if (peekIterator.hasNext()) {
            priorityQueue.add(peekIterator);
        }
        return next;
    }
}
