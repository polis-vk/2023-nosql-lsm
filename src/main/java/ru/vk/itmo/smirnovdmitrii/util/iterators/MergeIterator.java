package ru.vk.itmo.smirnovdmitrii.util.iterators;

import ru.vk.itmo.Entry;
import ru.vk.itmo.smirnovdmitrii.util.BinaryMinHeap;
import ru.vk.itmo.smirnovdmitrii.util.EqualsComparator;
import ru.vk.itmo.smirnovdmitrii.util.MinHeap;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

public class MergeIterator<T, E extends Entry<T>> implements Iterator<E> {
    private final EqualsComparator<T> comparator;
    private final MinHeap<PeekingIterator<E>> heap;

    private MergeIterator(
            final Collection<PeekingIterator<E>> iterators,
            final EqualsComparator<T> comparator
    ) {
        this.comparator = comparator;
        heap = new BinaryMinHeap<>(iterators, this::compare);
    }

    private int compare(
            final PeekingIterator<E> o1,
            final PeekingIterator<E> o2
    ) {
        final int keyCompare = comparator.compare(o1.peek().key(), o2.peek().key());
        if (keyCompare == 0) {
            return Integer.compare(o1.getId(), o2.getId());
        }
        return keyCompare;
    }

    @Override
    public boolean hasNext() {
        advance();
        return heap.min() != null;
    }

    private void add(final PeekingIterator<E> iterator) {
        if (iterator.hasNext()) {
            heap.add(iterator);
        }
    }

    @Override
    public E next() {
        advance();
        final PeekingIterator<E> iterator = heap.removeMin();
        final E result = iterator.next();
        add(iterator);
        final T key = result.key();
        while (!heap.isEmpty()) {
            final PeekingIterator<E> currentIterator = heap.min();
            if (!comparator.equals(currentIterator.peek().key(), key)) {
                break;
            }
            skip();
        }
        return result;
    }

    private void advance() {
        if (heap.isEmpty()) {
            return;
        }
        while (!heap.isEmpty() && heap.min().peek().value() == null) {
            final T key = heap.min().peek().key();
            while (!heap.isEmpty() && comparator.equals(heap.min().peek().key(), key)) {
                skip();
            }
        }
    }

    private void skip() {
        final PeekingIterator<E> iterator = heap.removeMin();
        iterator.next();
        add(iterator);
    }

    public static class Builder<T, E extends Entry<T>> {
        private final List<PeekingIterator<E>> list = new ArrayList<>();

        private final EqualsComparator<T> comparator;

        public Builder(final EqualsComparator<T> comparator) {
            this.comparator = comparator;
        }

        public Builder<T, E> addIterator(final PeekingIterator<E> iterator) {
            if (iterator.hasNext()) {
                list.add(iterator);
            }
            return this;
        }

        public MergeIterator<T, E> build() {
            return new MergeIterator<>(list, comparator);
        }
    }

}
