package ru.vk.itmo.kobyzhevaleksandr;

/**
 * An iterator that extends from {@link PeekIterator} and contains a priority value.
 *
 * @param <E> the type of elements returned by this iterator
 */
public interface PriorityPeekIterator<E> extends PeekIterator<E> {

    int priority();
}
