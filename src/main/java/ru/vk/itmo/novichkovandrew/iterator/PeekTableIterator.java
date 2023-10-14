package ru.vk.itmo.novichkovandrew.iterator;

import java.util.Iterator;

public class PeekTableIterator<T> implements PeekIterator<T>, TableIterator<T> {
    private final Iterator<T> iterator;
    private final int tableNumber;
    private T peeked;

    public PeekTableIterator(Iterator<T> iterator, int tableNumber) {
        this.iterator = iterator;
        this.tableNumber = tableNumber;
        if (iterator.hasNext()) {
            peeked = iterator.next();
        }
    }

    @Override
    public T peek() {
        return this.peeked;
    }

    @Override
    public boolean hasNext() {
        return peeked != null;
    }

    @Override
    public T next() {
        T value = peeked;
        peeked = iterator.hasNext() ? iterator.next() : null;
        return value;
    }

    @Override
    public int getTableNumber() {
        return tableNumber;
    }
}
