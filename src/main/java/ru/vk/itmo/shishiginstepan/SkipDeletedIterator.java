package ru.vk.itmo.shishiginstepan;

import ru.vk.itmo.Entry;

import java.lang.foreign.MemorySegment;
import java.util.Iterator;

public class SkipDeletedIterator implements Iterator<Entry<MemorySegment>> {
    private Entry<MemorySegment> prefetched;
    private final Iterator<Entry<MemorySegment>> iterator;

    public SkipDeletedIterator(
            Iterator<Entry<MemorySegment>> iterator
    ) {
        this.iterator = iterator;
    }

    @Override
    public boolean hasNext() {
        skipDeleted();
        return iterator.hasNext() || prefetched != null;
    }

    @Override
    public Entry<MemorySegment> next() {
        skipDeleted();
        if (prefetched == null) {
            return iterator.next();
        } else {
            Entry<MemorySegment> toReturn = prefetched;
            prefetched = null;
            return toReturn;
        }
    }

    public Entry<MemorySegment> peekNext() {
        if (prefetched == null) {
            prefetched = iterator.next();
        }
        return prefetched;
    }

    public void skipDeleted() {
        while (iterator.hasNext()) {
            Entry<MemorySegment> next = peekNext();
            if (next.value() == null) {
                prefetched = null;
            } else break;
        }
    }
}
