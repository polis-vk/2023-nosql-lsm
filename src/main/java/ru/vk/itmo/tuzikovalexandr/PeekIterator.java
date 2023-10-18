package ru.vk.itmo.tuzikovalexandr;

import ru.vk.itmo.Entry;

import java.lang.foreign.MemorySegment;
import java.util.Iterator;

public class PeekIterator implements Iterator<Entry<MemorySegment>> {

    private final long priority;
    private Entry<MemorySegment> currentEntry;
    private final Iterator<Entry<MemorySegment>> iterator;

    public PeekIterator(Iterator<Entry<MemorySegment>> iterator, long priority) {
        this.priority = priority;
        this.iterator = iterator;
    }

    @Override
    public boolean hasNext() {
        return currentEntry != null || iterator.hasNext();
    }

    @Override
    public Entry<MemorySegment> next() {
        Entry<MemorySegment> next = peek();
        currentEntry = null;
        return next;
    }

    public Entry<MemorySegment> peek() {
        if (currentEntry == null) {
            currentEntry = iterator.next();
        }
        return currentEntry;
    }

    public long getPriority() {
        return priority;
    }
}
