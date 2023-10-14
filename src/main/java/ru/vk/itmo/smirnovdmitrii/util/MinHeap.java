package ru.vk.itmo.smirnovdmitrii.util;

import java.util.Collection;

public interface MinHeap<T> {

    T min();

    T removeMin();

    void add(T t);

    default void addAll(Collection<T> collection) {
        for (final T t: collection) {
            add(t);
        }
    }

    boolean isEmpty();

}
