package ru.vk.itmo.cheshevandrey;

import java.util.Iterator;
import java.util.NoSuchElementException;

class PeekIterator<T> implements Iterator<T> {

    public final int id;
    private final Iterator<T> delegate;
    private T peek;

    PeekIterator(int id, Iterator<T> delegate) {
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

    T peek() {
        if (peek == null) {
            if (!delegate.hasNext()) {
                return null;
            }
            peek = delegate.next();
        }
        return peek;
    }
}
