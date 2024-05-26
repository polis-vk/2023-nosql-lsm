package ru.vk.itmo.podobaalexandr;

import ru.vk.itmo.Entry;

import java.lang.foreign.MemorySegment;
import java.util.Iterator;

public class IndexedPeekIterator implements Iterator<Entry<MemorySegment>> {

    private final Iterator<Entry<MemorySegment>> delegate;
    private final long index;
    private Entry<MemorySegment> peek;

    public IndexedPeekIterator(Iterator<Entry<MemorySegment>> delegate, long index) {
        this.delegate = delegate;
        this.index = index;
    }

    public long index() {
        return index;
    }

    public Entry<MemorySegment> peek() {
        if (peek == null && hasNext()) {
            peek = delegate.next();
        }
        return peek;
    }

    @Override
    public boolean hasNext() {
        return peek != null || delegate.hasNext();
    }

    @Override
    public Entry<MemorySegment> next() {
        Entry<MemorySegment> result = peek();
        peek = null;
        return result;
    }
}
