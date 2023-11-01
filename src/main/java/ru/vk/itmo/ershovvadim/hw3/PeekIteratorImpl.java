package ru.vk.itmo.ershovvadim.hw3;

import ru.vk.itmo.Entry;

import java.lang.foreign.MemorySegment;
import java.util.Iterator;
import java.util.NoSuchElementException;

public class PeekIteratorImpl implements PeekIterator<Entry<MemorySegment>> {
    protected final Iterator<Entry<MemorySegment>> iterator;
    protected final int priority;
    protected Entry<MemorySegment> currentEntry;

    protected PeekIteratorImpl(Iterator<Entry<MemorySegment>> iterator, int priority) {
        this.iterator = iterator;
        this.priority = priority;
    }

    @Override
    public Entry<MemorySegment> peek() throws NoSuchElementException {
        if (currentEntry == null) {
            currentEntry = iterator.next();
        }
        return currentEntry;
    }

    @Override
    public int getPriority() {
        return priority;
    }

    @Override
    public boolean hasNext() {
        return currentEntry != null || iterator.hasNext();
    }

    @Override
    public Entry<MemorySegment> next() throws NoSuchElementException {
        var peek = peek();
        currentEntry = null;
        return peek;
    }
}
