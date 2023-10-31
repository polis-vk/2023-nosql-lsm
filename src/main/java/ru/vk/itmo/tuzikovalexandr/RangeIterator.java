package ru.vk.itmo.tuzikovalexandr;

import java.util.*;

public class RangeIterator<T> implements Iterator<T> {

    private final Queue<PeekIterator<T>> iterators;
    private final Comparator<T> comparator;
    private PeekIterator<T> peek;

    public RangeIterator(Collection<Iterator<T>> peekIterators, Comparator<T> comparator) {
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
        while (peek == null) {
            peek = iterators.poll();
            if (peek == null) {
                return null;
            }

            while (true) {
                PeekIterator<T> next = iterators.peek();
                if (next == null) {
                    break;
                }

                int compare = comparator.compare(peek.peek(), next.peek());
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

            if (peek.peek() == null) {
                peek = null;
                continue;
            }

            if (skip(peek.peek())) {
                peek.next();
                if (peek.hasNext()) {
                    iterators.add(peek);
                }
                peek = null;
            }
        }

        return peek;
    }

    protected boolean skip(T t) {
        return false;
    }

    @Override
    public boolean hasNext() { return peek() != null; }

    @Override
    public T next() {
        PeekIterator<T> peek = peek();
        if (peek == null) {
            throw new NoSuchElementException();
        }
        T next = peek.next();
        this.peek = null;
        if (peek.hasNext()) {
            iterators.add(peek);
        }
        return next;
    }
}
