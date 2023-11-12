package ru.vk.itmo.dyagayalexandra;

import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.PriorityQueue;

public final class MergedIterator<E> implements Iterator<E> {

    private final PriorityQueue<IteratorWrapper<E>> iterators;
    private final Comparator<E> comparator;

    private MergedIterator(PriorityQueue<IteratorWrapper<E>> iterators, Comparator<E> comparator) {
        this.iterators = iterators;
        this.comparator = comparator;
    }

    public static <E> Iterator<E> createMergedIterator(List<Iterator<E>> iterators, Comparator<E> comparator) {
        if (iterators.isEmpty()) {
            return Collections.emptyIterator();
        }

        if (iterators.size() == 1) {
            return iterators.get(0);
        }

        PriorityQueue<MergedIterator.IteratorWrapper<E>> queue =
                new PriorityQueue<>(iterators.size(), (iterator1, iterator2) -> {
            int result = comparator.compare(iterator1.peek(), iterator2.peek());
            if (result != 0) {
                return result;
            }

            return Integer.compare(iterator1.index, iterator2.index);
        });

        int index = 0;
        for (Iterator<E> iterator : iterators) {
            if (iterator == null) {
                continue;
            }

            if (iterator.hasNext()) {
                queue.add(new IteratorWrapper<>(index++, iterator));
            }
        }

        return new MergedIterator<>(queue, comparator);
    }

    @Override
    public boolean hasNext() {
        return !iterators.isEmpty();
    }

    @Override
    public E next() {
        IteratorWrapper<E> iterator = iterators.remove();
        E next = iterator.next();
        while (!iterators.isEmpty()) {
            IteratorWrapper<E> candidate = iterators.peek();
            if (comparator.compare(next, candidate.peek()) != 0) {
                break;
            }

            iterators.remove();
            candidate.next();
            if (candidate.hasNext()) {
                iterators.add(candidate);
            }
        }

        if (iterator.hasNext()) {
            iterators.add(iterator);
        }

        return next;
    }

    private static class IteratorWrapper<E> extends PeekingIterator<E> {

        final int index;

        public IteratorWrapper(int index, Iterator<E> iterator) {
            super(iterator);
            this.index = index;
        }

    }
}
