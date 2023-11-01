package ru.vk.itmo.reshetnikovaleksei.iterators;

import ru.vk.itmo.Entry;

import java.lang.foreign.MemorySegment;
import java.util.Iterator;

public class PeekingIterator implements Iterator<Entry<MemorySegment>> {
    private final Iterator<Entry<MemorySegment>> iterator;
    private final int priority;

    private Entry<MemorySegment> next;

    public PeekingIterator(Iterator<Entry<MemorySegment>> iterator) {
        this(iterator, 0);
    }

    public PeekingIterator(Iterator<Entry<MemorySegment>> iterator, int priority) {
        this.iterator = iterator;
        this.priority = priority;

        if (iterator.hasNext()) {
            next = iterator.next();
        }
    }

    @Override
    public boolean hasNext() {
        return next != null || iterator.hasNext();
    }

    @Override
    public Entry<MemorySegment> next() {
        Entry<MemorySegment> toReturn = peek();
        next = null;

        return toReturn;
    }

    public int priority() {
        return priority;
    }

    public Entry<MemorySegment> peek() {
        if (next == null) {
            next = iterator.next();
        }

        return next;
    }
}
