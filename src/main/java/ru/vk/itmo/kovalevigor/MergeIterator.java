package ru.vk.itmo.kovalevigor;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.ArrayList;

public class MergeIterator<E> implements Iterator<E> {

    protected final PriorityQueue<Iterator<E>> queue;

    public MergeIterator(final Collection<? extends Iterator<E>> collection) {
        final List<Iterator<E>> filteredCollection = new ArrayList<>(collection.size());
        for (final Iterator<E> iterator : collection) {
            if (iterator.hasNext()) {
                filteredCollection.add(iterator);
            }
        }
        queue = new PriorityQueue<>(filteredCollection);
    }

    @Override
    public boolean hasNext() {
        return !queue.isEmpty();
    }

    @Override
    public E next() {
        final Iterator<E> iterator = queue.remove();

        Iterator<E> nextIterator = queue.peek();
        while (nextIterator != null && cmp(iterator, nextIterator) == 0) {
            shiftAdd(queue.poll());
            nextIterator = queue.peek();
        }
        return shiftAdd(iterator);
    }

    private E shiftAdd(final Iterator<E> iterator) {
        final E value = iterator.next();
        if (iterator.hasNext()) {
            queue.add(iterator);
        }
        return value;
    }

    @SuppressWarnings("unchecked")
    private <T extends Iterator<E>> int cmp(final T lhs, final T rhs) {
        return queue.comparator() == null
                ? ((Comparable<T>) lhs).compareTo(rhs)
                : queue.comparator().compare(lhs, rhs);
    }
}
