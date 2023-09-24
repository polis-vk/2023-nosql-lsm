package ru.vk.itmo.svistukhinandrey;

import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;

import java.lang.foreign.MemorySegment;
import java.util.Iterator;
import java.util.NavigableMap;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentSkipListMap;

public class InMemoryDao implements Dao<MemorySegment, Entry<MemorySegment>> {

    private final NavigableMap<MemorySegment, Entry<MemorySegment>> memorySegmentTreeMap;
    private static final Iterator<Entry<MemorySegment>> EMPTY_ITERATOR = new Iterator<>() {
        @Override
        public boolean hasNext() {
            return false;
        }

        @Override
        public Entry<MemorySegment> next() {
            throw new NoSuchElementException();
        }
    };

    public InMemoryDao() {
        memorySegmentTreeMap = new ConcurrentSkipListMap<>(new MemorySegmentComparator());
    }

    @Override
    public Entry<MemorySegment> get(MemorySegment key) {
        return memorySegmentTreeMap.get(key);
    }

    @Override
    public Iterator<Entry<MemorySegment>> get(MemorySegment from, MemorySegment to) {
        if (memorySegmentTreeMap.isEmpty()) {
            return EMPTY_ITERATOR;
        }

        if (from == null) {
            from = memorySegmentTreeMap.firstKey();
        }

        boolean last = false;
        if (to == null) {
            to = memorySegmentTreeMap.lastKey();
            last = true;
        }

        return memorySegmentTreeMap.subMap(from, true, to, last).values().iterator();
    }

    @Override
    public synchronized void upsert(Entry<MemorySegment> entry) {
        memorySegmentTreeMap.put(entry.key(), entry);
    }
}
