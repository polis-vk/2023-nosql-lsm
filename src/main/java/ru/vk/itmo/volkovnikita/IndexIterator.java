package ru.vk.itmo.volkovnikita;

import ru.vk.itmo.Entry;

import java.lang.foreign.MemorySegment;
import java.util.Iterator;

public class IndexIterator implements Iterator<Entry<MemorySegment>> {

    private final Iterator<Entry<MemorySegment>> iterator;
    private final int index;
    protected Entry<MemorySegment> peek;

    public IndexIterator(int index, Iterator<Entry<MemorySegment>> iterator) {
        this.index = index;
        this.iterator = iterator;
    }

    public int order() {
        return index;
    }

    public Entry<MemorySegment> peek() {
        if (peek == null && iterator.hasNext()) {
            peek = iterator.next();
        }
        return peek;
    }

    @Override
    public boolean hasNext() {
        return peek != null || iterator.hasNext();
    }

    @Override
    public Entry<MemorySegment> next() {
        Entry<MemorySegment> result = peek();
        peek = null;
        return result;
    }
}
