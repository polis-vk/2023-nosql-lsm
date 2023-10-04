package ru.vk.itmo.proninvalentin;

import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;
import java.lang.foreign.MemorySegment;
import java.util.Iterator;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class InMemoryDao implements Dao<MemorySegment, Entry<MemorySegment>> {
    private final NavigableMap<MemorySegment, Entry<MemorySegment>> memorySegments;

    public InMemoryDao() {
        memorySegments = new ConcurrentSkipListMap<>(new MemorySegmentComparator());
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
    public Iterator<Entry<MemorySegment>> get(MemorySegment from, MemorySegment to) {
        if (from == null && to == null) {
            return all();
        } else if (to == null) {
            return allFrom(from);
        } else if (from == null) {
            return allTo(to);
        } else {
            return memorySegments.tailMap(from).headMap(to).values().iterator();
        }
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
