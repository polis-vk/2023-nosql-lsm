package ru.vk.itmo.tuzikovalexandr;

import java.util.Iterator;
import java.util.NoSuchElementException;

public class PeekIterator<T> implements Iterator<T> {

    private final int priority;
    private T currentEntry;
    private final Iterator<T> iterator;

    public PeekIterator(Iterator<T> iterator, int priority) {
        this.priority = priority;
        this.iterator = iterator;
    }

    @Override
    public boolean hasNext() {
        return currentEntry != null || iterator.hasNext();
    }

    @Override
    public T next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        T next = peek();
        currentEntry = null;
        return next;
    }

    public T peek() {
        if (currentEntry == null) {
            if (!iterator.hasNext()) {
                return null;
            }
            currentEntry = iterator.next();
        }
        return currentEntry;
    }

    public int getPriority() {
        return priority;
    }
}
