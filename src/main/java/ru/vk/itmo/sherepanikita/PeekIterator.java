package ru.vk.itmo.sherepanikita;

import ru.vk.itmo.Entry;

import java.lang.foreign.MemorySegment;
import java.util.Iterator;
import java.util.NavigableMap;

public class PeekIterator implements Iterator<Entry<MemorySegment>> {

    private final Iterator<Entry<MemorySegment>> iterator;
    private Entry<MemorySegment> currentEntry;

    PeekIterator(Iterator<Entry<MemorySegment>> iterator) {
        this.iterator = iterator;
    }

    @Override
    public boolean hasNext() {
        return iterator.hasNext();
    }

    @Override
    public Entry<MemorySegment> next() {
        currentEntry = iterator.next();
        return currentEntry;
    }

    public Entry<MemorySegment> getCurrentEntry() {
        return currentEntry;
    }
}
