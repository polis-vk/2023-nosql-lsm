package ru.vk.itmo.sherepanikita;

import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;

import java.lang.foreign.MemorySegment;
import java.util.Iterator;
import java.util.concurrent.ConcurrentSkipListMap;

public class InMemoryDao implements Dao<MemorySegment, Entry<MemorySegment>> {

    private final ConcurrentSkipListMap<MemorySegment, Entry<MemorySegment>> segments;

    public InMemoryDao() {
        segments = new ConcurrentSkipListMap<>(new MemorySegmentComparator());
    }

    @Override
    public Iterator<Entry<MemorySegment>> get(MemorySegment from, MemorySegment to) {
        if (from == null && to == null) {
            return all();
        } else if (from == null) {
            return allTo(to);
        } else if (to == null) {
            return allFrom(from);
        } else {
            return allBetween(from, to);
        }
    }

    @Override
    public Entry<MemorySegment> get(MemorySegment key) {
        return segments.get(key);
    }

    @Override
    public Iterator<Entry<MemorySegment>> allFrom(MemorySegment from) {
        return segments.tailMap(from).values().iterator();
    }

    @Override
    public Iterator<Entry<MemorySegment>> allTo(MemorySegment to) {
        return segments.headMap(to).values().iterator();
    }

    @Override
    public Iterator<Entry<MemorySegment>> all() {
        return segments.values().iterator();
    }

    private Iterator<Entry<MemorySegment>> allBetween(MemorySegment from, MemorySegment to) {
        return segments.subMap(from, to).values().iterator();
    }

    @Override
    public void upsert(Entry<MemorySegment> entry) {
        if (entry == null) {
            throw new IllegalArgumentException("Incoming entry is NULL");
        } else {
            segments.put(entry.key(), entry);
        }
    }
}
