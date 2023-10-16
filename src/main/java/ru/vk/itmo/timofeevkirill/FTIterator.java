package ru.vk.itmo.timofeevkirill;

import ru.vk.itmo.Entry;

import java.lang.foreign.MemorySegment;
import java.util.Collection;
import java.util.Iterator;
import java.util.NavigableMap;
import java.util.NoSuchElementException;

public class FTIterator implements Iterator<Entry<MemorySegment>> {
    private final Iterator<Entry<MemorySegment>> iterator;

    public FTIterator(NavigableMap<MemorySegment, Entry<MemorySegment>> map, MemorySegment from, MemorySegment to) {
        Collection<Entry<MemorySegment>> fromToMap;
        if (from == null && to == null) {
            fromToMap = map.values();
        } else if (from == null) {
            fromToMap = map.headMap(to).values();
        } else if (to == null) {
            fromToMap = map.tailMap(from).values();
        } else {
            fromToMap = map.tailMap(from).headMap(to).values();
        }
        this.iterator = fromToMap.iterator();
    }

    @Override
    public boolean hasNext() {
        return iterator.hasNext();
    }

    @Override
    public Entry<MemorySegment> next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        return iterator.next();
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException("Remove is not supported");
    }
}
