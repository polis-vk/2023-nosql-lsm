package ru.vk.itmo.abramovilya;

import ru.vk.itmo.Entry;

import java.lang.foreign.MemorySegment;
import java.util.Iterator;
import java.util.NavigableMap;

public class MemTable implements Table {
    private final Iterator<Entry<MemorySegment>> iterator;
    private Entry<MemorySegment> current;

    MemTable(NavigableMap<MemorySegment, Entry<MemorySegment>> map) {
        this.iterator = map.values().iterator();
        current = iterator.next();
    }

    @Override
    public MemorySegment getValue() {
        return current.value();
    }

    @Override
    public MemorySegment getKey() {
        return current.key();
    }

    @Override
    public MemorySegment nextKey() {
        if (!iterator.hasNext()) {
            return null;
        }
        current = iterator.next();
        return current.key();
    }

    @Override
    public void close() {
    }

    @Override
    public int number() {
        return Integer.MAX_VALUE;
    }
}
