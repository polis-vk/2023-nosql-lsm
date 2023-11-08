package ru.vk.itmo.dalbeevgeorgii;

import ru.vk.itmo.Entry;

import java.lang.foreign.MemorySegment;
import java.util.Iterator;

public class IndexedPeekIterator implements Iterator<Entry<MemorySegment>> {

    protected final Iterator<Entry<MemorySegment>> delegate;
    private final int index;
    protected Entry<MemorySegment> peek;

    public IndexedPeekIterator(int index, Iterator<Entry<MemorySegment>> delegate) {
        this.index = index;
        this.delegate = delegate;
    }

    public int order() {
        return index;
    }

    public Entry<MemorySegment> peek() {
        if (peek == null && delegate.hasNext()) {
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
