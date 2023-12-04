package ru.vk.itmo.prokopyevnikita;

import ru.vk.itmo.Entry;

import java.lang.foreign.MemorySegment;
import java.util.Iterator;

public class OrderedPeekIteratorImpl implements OrderedPeekIterator<Entry<MemorySegment>> {

    private final int order;
    protected final Iterator<Entry<MemorySegment>> delegate;
    protected Entry<MemorySegment> peek;

    public OrderedPeekIteratorImpl(int order, Iterator<Entry<MemorySegment>> delegate) {
        this.order = order;
        this.delegate = delegate;
    }

    @Override
    public int order() {
        return order;
    }

    @Override
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

