package ru.vk.itmo.pashchenkoalexandr;

import java.util.*;

public class MergeIterator<T> implements Iterator<T> {

    private final PriorityQueue<PeekIterator<T>> priorityQueue;

    private static class PeekIterator<T> implements Iterator<T> {

        private final Iterator<T> delegate;
        private T peek;

        private PeekIterator(Iterator<T> delegate) {
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

    public MergeIterator(Collection<Iterator<T>> iterators, Comparator<T> comparator) {
        Comparator<PeekIterator<T>> peekComp = (o1, o2) -> comparator.compare(o1.peek(), o2.peek());
        peekComp.thenComparing(

        )
        priorityQueue = new PriorityQueue<>(
                iterators.size(),
                peekComp
        );
    }


    @Override
    public boolean hasNext() {
        return priorityQueue.is;
    }

    @Override
    public T next() {
        return null;
    }
}
