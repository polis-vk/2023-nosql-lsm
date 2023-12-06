package ru.vk.itmo.prokopyevnikita;

import java.util.Iterator;

public interface OrderedPeekIterator<E> extends Iterator<E> {
    int order();

    E peek();
}
