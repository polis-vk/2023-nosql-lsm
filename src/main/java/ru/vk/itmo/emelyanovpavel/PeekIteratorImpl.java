package ru.vk.itmo.emelyanovpavel;

import ru.vk.itmo.Entry;

import java.lang.foreign.MemorySegment;
import java.util.Iterator;
import java.util.NoSuchElementException;

public class PeekIteratorImpl implements PeekIterator<Entry<MemorySegment>> {
    private final Iterator<Entry<MemorySegment>> iterator;
    protected final int priority;
    private Entry<MemorySegment> currentData;

    protected PeekIteratorImpl(Iterator<Entry<MemorySegment>> iterator, int priority) {
        this.iterator = iterator;
        this.priority = priority;
    }

    @Override
    public Entry<MemorySegment> peek() {
        if (currentData == null) {
            if (!iterator.hasNext()) {
                throw new NoSuchElementException();
            }
            currentData = iterator.next();
        }
        return currentData;
    }

    @Override
    public int getPriority() {
        return priority;
    }

    @Override
    public boolean hasNext() {
        return iterator.hasNext() || currentData != null;
    }

    @Override
    public Entry<MemorySegment> next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        Entry<MemorySegment> nextEntry = peek();
        currentData = null;
        return nextEntry;
    }
}
