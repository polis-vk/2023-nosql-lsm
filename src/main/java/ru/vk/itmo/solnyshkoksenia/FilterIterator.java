package ru.vk.itmo.solnyshkoksenia;

import ru.vk.itmo.Entry;

import java.lang.foreign.MemorySegment;
import java.util.Iterator;

public class FilterIterator implements Iterator<Entry<MemorySegment>> {
    private final Iterator<Triple<MemorySegment>> innerIterator;

    public FilterIterator(Iterator<Triple<MemorySegment>> innerIterator) {
        this.innerIterator = innerIterator;
    }

    @Override
    public boolean hasNext() {
        return innerIterator.hasNext();
    }

    @Override
    public Entry<MemorySegment> next() {
        return innerIterator.next();
    }
}
