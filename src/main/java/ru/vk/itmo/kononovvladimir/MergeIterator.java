package ru.vk.itmo.kononovvladimir;

import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.PriorityQueue;

public class MergeIterator<T> implements Iterator<T> {

    private final PriorityQueue<PeekIterator<T>> priorityQueue;
    private final Comparator<T> comparator;
    PeekIterator<T> peekIterator;

    private static class PeekIterator<T> implements Iterator<T> {

        public final int id;
        private final Iterator<T> delegate;
        private T iteratorPeek;

        private PeekIterator(int id, Iterator<T> delegate) {
            this.id = id;
            this.delegate = delegate;
        }

        @Override
        public boolean hasNext() {
            if (iteratorPeek == null) {
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
            this.iteratorPeek = null;
            return peek;
        }

        private T peek() {
            if (iteratorPeek == null) {
                if (!delegate.hasNext()) {
                    return null;
                }
                iteratorPeek = delegate.next();
            }
            return iteratorPeek;
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

            PeekIterator<T> next;
            do {
                next = priorityQueue.peek();
            } while (updatePeek(next));

            if (peekIterator.peek() == null) {
                peekIterator = null;
                continue;
            }

            if (skip(peekIterator.peek())) {
                peekIterator.next();
                if (peekIterator.hasNext()) {
                    priorityQueue.add(peekIterator);
                }
                peekIterator = null;
            }
        }

        return peekIterator;
    }

    //Спасибо код климату за мемы
    private boolean updatePeek(PeekIterator<T> next) {

        if (next == null) {
            return false;
        }

        int compare = comparator.compare(peekIterator.peek(), next.peek());
        if (compare == 0) {
            PeekIterator<T> poll = priorityQueue.poll();
            if (poll != null) {
                poll.next();
                if (poll.hasNext()) {
                    priorityQueue.add(poll);
                }
            }
            return true;
        } else {
            return false;
        }
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
        PeekIterator<T> peek = peek();
        if (peek == null) {
            throw new NoSuchElementException();
        }
        T next = peek.next();
        this.peekIterator = null;
        if (peek.hasNext()) {
            priorityQueue.add(peek);
        }
        return next;
    }
}
