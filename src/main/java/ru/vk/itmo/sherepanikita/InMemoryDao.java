package ru.vk.itmo.sherepanikita;

import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;

import java.lang.foreign.MemorySegment;
import java.util.Iterator;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class InMemoryDao implements Dao<MemorySegment, Entry<MemorySegment>> {

    private final NavigableMap<MemorySegment, Entry<MemorySegment>> segments;

    public InMemoryDao() {
        segments = new ConcurrentSkipListMap<>(new MemorySegmentComparator());
    }

    @Override
    public Iterator<Entry<MemorySegment>> get(MemorySegment from, MemorySegment to) {
        if (from == null && to == null) {
            return segments.values().iterator();
        } else if (from == null) {
            return segments.headMap(to).values().iterator();
        } else if (to == null) {
            return segments.tailMap(from).values().iterator();
        }
        return segments.subMap(from, to).values().iterator();
    }

    @Override
    public Entry<MemorySegment> get(MemorySegment key) {
        return segments.get(key);
    }

    @Override
    public void upsert(Entry<MemorySegment> entry) {
        if (entry == null) {
            throw new IllegalArgumentException("Incoming entry is NULL");
        }
        segments.put(entry.key(), entry);
    }

}
