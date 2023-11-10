package ru.vk.itmo.proninvalentin;

import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;
import ru.vk.itmo.proninvalentin.comparators.MemorySegmentComparator;
import ru.vk.itmo.proninvalentin.iterators.PeekingPriorityIteratorImpl;

import java.lang.foreign.MemorySegment;
import java.util.Iterator;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class InMemoryDao implements Dao<MemorySegment, Entry<MemorySegment>> {
    private final NavigableMap<MemorySegment, Entry<MemorySegment>> memorySegments;

    public InMemoryDao() {
        memorySegments = new ConcurrentSkipListMap<>(MemorySegmentComparator.getInstance());
    }

    @Override
    public Iterator<Entry<MemorySegment>> all() {
        return memorySegments.values().iterator();
    }

    @Override
    public Iterator<Entry<MemorySegment>> allFrom(MemorySegment from) {
        return memorySegments.tailMap(from).values().iterator();
    }

    @Override
    public Iterator<Entry<MemorySegment>> allTo(MemorySegment to) {
        return memorySegments.headMap(to).values().iterator();
    }

    @Override
    public PeekingPriorityIteratorImpl get(MemorySegment from, MemorySegment to) {
        Iterator<Entry<MemorySegment>> memoryIterator;
        if (from == null && to == null) {
            memoryIterator = all();
        } else if (to == null) {
            memoryIterator = allFrom(from);
        } else if (from == null) {
            memoryIterator = allTo(to);
        } else {
            memoryIterator = memorySegments.tailMap(from).headMap(to).values().iterator();
        }

        return new PeekingPriorityIteratorImpl(memoryIterator, 0);
    }

    @Override
    public Entry<MemorySegment> get(MemorySegment key) {
        return memorySegments.get(key);
    }

    @Override
    public void upsert(Entry<MemorySegment> entry) {
        memorySegments.put(entry.key(), entry);
    }
}
