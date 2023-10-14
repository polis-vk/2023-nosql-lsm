package ru.vk.itmo.novichkovandrew.iterator;

import java.util.Iterator;

public interface PeekIterator<T> extends Iterator<T> {
    T peek();
}
