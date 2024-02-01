package ru.vk.itmo.dyagayalexandra;

import java.util.Iterator;

public class PeekingIterator<E> implements Iterator<E> {

    private final Iterator<E> iterator;
    private E peekedEntry;

    public PeekingIterator(Iterator<E> iterator) {
        this.iterator = iterator;
    }

    public E peek() {
        if (peekedEntry == null && iterator.hasNext()) {
            peekedEntry = iterator.next();
        }

        return peekedEntry;
    }

    @Override
    public boolean hasNext() {
        return peekedEntry != null || iterator.hasNext();
    }

    @Override
    public E next() {
        E result = peek();
        peekedEntry = null;
        return result;
    }
}
