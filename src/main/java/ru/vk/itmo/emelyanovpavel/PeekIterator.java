package ru.vk.itmo.emelyanovpavel;

import java.util.Iterator;

public interface PeekIterator<E> extends Iterator<E> {
    E peek();

    int getPriority();
}
