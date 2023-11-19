package ru.vk.itmo.dyagayalexandra;

import ru.vk.itmo.Entry;

import java.lang.foreign.MemorySegment;
import java.util.Iterator;

public class PeekingIterator implements Iterator<Entry<MemorySegment>> {

    private final int index;
    private final Iterator<Entry<MemorySegment>> iterator;
    private Entry<MemorySegment> peekedEntry;

    public PeekingIterator(int index, Iterator<Entry<MemorySegment>> iterator) {
        this.index = index;
        this.iterator = iterator;
    }

    public int getIndex() {
        return index;
    }

    public Entry<MemorySegment> peek() {
        if (peekedEntry == null && iterator.hasNext()) {
            peekedEntry = iterator.next();
        }

        return peekedEntry;
    }

    @Override
    public boolean hasNext() {
        return peekedEntry != null || iterator.hasNext();
    }

    @Override
    public Entry<MemorySegment> next() {
        Entry<MemorySegment> result = peek();
        peekedEntry = null;
        return result;
    }
}
