package ru.vk.itmo.cheshevandrey;

import java.util.Iterator;
import java.util.NoSuchElementException;

class PeekIterator<T> implements Iterator<T> {

    public final int id;
    private final Iterator<T> delegate;
    private T currIterator;

    PeekIterator(int id, Iterator<T> delegate) {
        this.id = id;
        this.delegate = delegate;
    }

    @Override
    public boolean hasNext() {
        if (currIterator == null) {
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
        this.currIterator = null;
        return peek;
    }

    T peek() {
        if (currIterator == null) {
            if (!delegate.hasNext()) {
                return null;
            }
            currIterator = delegate.next();
        }
        return currIterator;
    }
}
