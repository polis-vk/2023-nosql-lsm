package ru.vk.itmo.novichkovandrew.iterator;

import java.util.Comparator;

public class IteratorsComparator<T> implements Comparator<PeekTableIterator<T>> {
    private final Comparator<T> typeComparator;

    public IteratorsComparator(Comparator<T> typeComparator) {
        this.typeComparator = typeComparator;
    }

    @Override
    public int compare(PeekTableIterator<T> o1, PeekTableIterator<T> o2) {
        int memoryComparison = typeComparator.compare(o1.peek(), o2.peek());
        if (memoryComparison == 0) {
            return -1 * Integer.compare(o1.getTableNumber(), o2.getTableNumber());
        }
        return memoryComparison;
    }
}
