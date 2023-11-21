package ru.vk.itmo.proninvalentin.iterators;

import ru.vk.itmo.Entry;
import ru.vk.itmo.proninvalentin.comparators.MemorySegmentComparator;

import java.lang.foreign.MemorySegment;
import java.util.Iterator;

public class PeekingPriorityIteratorImpl implements PeekingPriorityIterator {
    private final Iterator<Entry<MemorySegment>> iterator;
    private Entry<MemorySegment> current;
    private final int priority;

    public PeekingPriorityIteratorImpl(Iterator<Entry<MemorySegment>> iterator, int priority) {
        this.iterator = iterator;
        this.priority = priority;
    }

    @Override
    public boolean hasNext() {
        return iterator.hasNext();
    }

    @Override
    public Entry<MemorySegment> next() {
        current = iterator.next();
        return current;
    }

    @Override
    public Entry<MemorySegment> getCurrent() {
        return current;
    }

    @Override
    public int getPriority() {
        return priority;
    }

    @Override
    public int compareTo(PeekingPriorityIterator another) {
        var compareResult = MemorySegmentComparator.getInstance()
                .compare(this.current.key(), another.getCurrent().key());
        if (compareResult != 0) {
            return compareResult;
        }
        return Integer.compare(this.priority, another.getPriority());
    }
}
