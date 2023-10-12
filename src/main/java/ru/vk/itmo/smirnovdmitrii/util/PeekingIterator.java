package ru.vk.itmo.smirnovdmitrii.util;

import java.util.Iterator;

public interface PeekingIterator<T> extends Iterator<T> {
    T peek();

    int getId();
}
