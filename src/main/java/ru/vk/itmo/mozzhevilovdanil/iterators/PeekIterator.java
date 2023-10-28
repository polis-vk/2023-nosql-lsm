package ru.vk.itmo.mozzhevilovdanil.iterators;

import java.util.Iterator;

public class PeekIterator<T> implements Iterator<T> {
    private final long id;
    private final Iterator<T> iterator;
    private T peek;

    public PeekIterator(Iterator<T> iterator, long id) {
        this.iterator = iterator;
        this.id = id;
    }

    @Override
    public boolean hasNext() {
        if (peek != null) {
            return true;
        }
        if (iterator.hasNext()) {
            peek = iterator.next();
            return true;
        }
        return false;
    }

    @Override
    public T next() {
        if (!hasNext()) {
            return null;
        }
        T result = peek;
        peek = null;
        return result;
    }

    public T peek() {
        if (!hasNext()) {
            return null;
        }
        return peek;
    }

    public long getId() {
        return id;
    }
}
