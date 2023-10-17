package ru.vk.itmo.chebotinalexandr;

import java.util.Iterator;

public interface PeekingIterator<E> extends Iterator<E> {
    E peek();

    default int priority() {
        return 0;
    }
}
