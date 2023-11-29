package ru.vk.itmo.tuzikovalexandr;

import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.PriorityQueue;
import java.util.Queue;

public abstract class RangeIterator<T> implements Iterator<T> {

    private final Queue<PeekIterator<T>> iterators;
    private final Comparator<T> comparator;
    private PeekIterator<T> peekIterator;

    protected RangeIterator(Collection<Iterator<T>> peekIterators, Comparator<T> comparator) {
        this.comparator = comparator;
        Comparator<PeekIterator<T>> peekComp = (o1, o2) -> comparator.compare(o1.peek(), o2.peek());
        iterators = new PriorityQueue<>(peekIterators.size(), peekComp.thenComparing(o -> -o.getPriority()));

        int priority = 0;
        for (Iterator<T> iterator : peekIterators) {
            if (iterator.hasNext()) {
                iterators.add(new PeekIterator<>(iterator, priority++));
            }
        }
    }

    private PeekIterator<T> peek() {
        while (peekIterator == null) {
            peekIterator = iterators.poll();
            if (peekIterator == null) {
                return null;
            }

            skipOldEntries();

            skipNullEntries();
        }

        return peekIterator;
    }

    private void skipOldEntries() {
        while (true) {
            PeekIterator<T> next = iterators.peek();
            if (next == null) {
                break;
            }

            int compare = comparator.compare(peekIterator.peek(), next.peek());
            if (compare == 0) {
                PeekIterator<T> poll = iterators.poll();
                if (poll != null) {
                    poll.next();
                    if (poll.hasNext()) {
                        iterators.add(poll);
                    }
                }
            } else {
                break;
            }
        }
    }

    private void skipNullEntries() {
        if (peekIterator.peek() == null) {
            peekIterator = null;
            return;
        }

        if (skip(peekIterator.peek())) {
            peekIterator.next();
            if (peekIterator.hasNext()) {
                iterators.add(peekIterator);
            }
            peekIterator = null;
        }
    }

    protected abstract boolean skip(T entry);

    @Override
    public boolean hasNext() {
        return peek() != null;
    }

    @Override
    public T next() {
        PeekIterator<T> localPeekIterator = peek();
        if (localPeekIterator == null) {
            throw new NoSuchElementException();
        }
        T next = localPeekIterator.next();
        this.peekIterator = null;
        if (localPeekIterator.hasNext()) {
            iterators.add(localPeekIterator);
        }
        return next;
    }
}
