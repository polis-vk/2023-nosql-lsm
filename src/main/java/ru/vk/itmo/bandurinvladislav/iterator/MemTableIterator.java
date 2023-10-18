package ru.vk.itmo.bandurinvladislav.iterator;

import ru.vk.itmo.Entry;

import java.lang.foreign.MemorySegment;
import java.util.concurrent.ConcurrentNavigableMap;

public class MemTableIterator implements MemorySegmentIterator {
    private final ConcurrentNavigableMap<MemorySegment, Entry<MemorySegment>> memTable;
    private Entry<MemorySegment> minKeyEntry;

    public MemTableIterator(ConcurrentNavigableMap<MemorySegment, Entry<MemorySegment>> memTable) {
        this.memTable = memTable;
        this.minKeyEntry = memTable.pollFirstEntry().getValue();
    }

    @Override
    public boolean hasNext() {
        return !memTable.isEmpty();
    }

    @Override
    public Entry<MemorySegment> next() {
        var result = minKeyEntry;
        minKeyEntry = memTable.pollFirstEntry().getValue();
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
