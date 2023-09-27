package ru.vk.itmo.gamzatgadzhimagomedov;

import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;

import java.lang.foreign.MemorySegment;
import java.util.Iterator;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class InMemoryDao implements Dao<MemorySegment, Entry<MemorySegment>> {

    private final ConcurrentNavigableMap<MemorySegment, Entry<MemorySegment>> mem;

    public InMemoryDao() {
        this.mem = new ConcurrentSkipListMap<>(new MemorySegmentComparator());
    }

    private NavigableMap<MemorySegment, Entry<MemorySegment>> subMap(MemorySegment from, MemorySegment to) {
        if (from != null && to != null) {
            return mem.subMap(from, to);
        }
        if (from != null) {
            return mem.tailMap(from, true);
        }
        if (to != null) {
            return mem.headMap(to, false);
        }
        return mem;
    }

    @Override
    public Iterator<Entry<MemorySegment>> get(MemorySegment from, MemorySegment to) {
        return subMap(from, to).values().iterator();
    }

    @Override
    public Entry<MemorySegment> get(MemorySegment key) {
        return mem.get(key);
    }

    @Override
    public void upsert(Entry<MemorySegment> entry) {
        mem.put(entry.key(), entry);
    }
}
