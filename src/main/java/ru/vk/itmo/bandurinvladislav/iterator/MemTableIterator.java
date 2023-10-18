package ru.vk.itmo.bandurinvladislav.iterator;

import ru.vk.itmo.Entry;

import java.lang.foreign.MemorySegment;
import java.util.Iterator;

public class MemTableIterator implements MemorySegmentIterator {
    private final Iterator<Entry<MemorySegment>> memTableIterator;
    private Entry<MemorySegment> minKeyEntry;

    public MemTableIterator(Iterator<Entry<MemorySegment>> memTableIterator) {
        this.memTableIterator = memTableIterator;
        if (memTableIterator.hasNext()) {
            this.minKeyEntry = memTableIterator.next();
        }
    }

    @Override
    public boolean hasNext() {
        return minKeyEntry != null;
    }

    @Override
    public Entry<MemorySegment> next() {
        var result = minKeyEntry;
        if (!memTableIterator.hasNext()) {
            minKeyEntry = null;
        } else {
            minKeyEntry = memTableIterator.next();
        }
        return result;
    }

    @Override
    public Entry<MemorySegment> peek() {
        return minKeyEntry;
    }

    @Override
    public int getPriority() {
        return 0;
    }
}
