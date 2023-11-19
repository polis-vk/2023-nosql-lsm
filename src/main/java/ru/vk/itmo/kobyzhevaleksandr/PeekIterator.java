package ru.vk.itmo.kobyzhevaleksandr;

import java.util.Iterator;

/**
 * An iterator that allows you to peek at a value (similar to the {@link java.util.Queue#peek()}
 * in {@link java.util.Queue}).
 *
 * @param <E> the type of elements returned by this iterator
 */
public interface PeekIterator<E> extends Iterator<E> {

    E peek();
}
