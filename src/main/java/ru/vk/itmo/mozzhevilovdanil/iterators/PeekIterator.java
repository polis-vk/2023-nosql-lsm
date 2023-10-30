package ru.vk.itmo.mozzhevilovdanil.iterators;

import java.util.Iterator;

public class PeekIterator<T> implements Iterator<T> {
    private final long id;
    private final Iterator<T> delegateIterator;
    private T peek;

    public PeekIterator(Iterator<T> iterator, long id) {
        this.delegateIterator = iterator;
        this.id = id;
    }

    @Override
    public boolean hasNext() {
        return peek != null || delegateIterator.hasNext();
    }

    @Override
    public T next() {
        updatePeek();
        T result = peek;
        peek = null;
        return result;
    }

    public T peek() {
        updatePeek();
        return peek;
    }

    private void updatePeek() {
        if (peek == null && delegateIterator.hasNext()) {
            peek = delegateIterator.next();
        }
    }

    public long getId() {
        return id;
    }
}
