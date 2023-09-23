package ru.vk.itmo.kobyzhevaleksandr;

import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;

import java.lang.foreign.MemorySegment;
import java.util.Iterator;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class InMemoryDao implements Dao<MemorySegment, Entry<MemorySegment>> {

    private final ConcurrentNavigableMap<MemorySegment, Entry<MemorySegment>> map =
            new ConcurrentSkipListMap<>(new MemorySegmentComparator());

    @Override
    public Iterator<Entry<MemorySegment>> get(MemorySegment from, MemorySegment to) {
        if (from == null && to == null) {
            return all();
        } else if (from == null) {
            return allTo(to);
        } else if (to == null) {
            return allFrom(from);
        }
        return map.subMap(from, to).values().iterator();
    }

    @Override
    public Entry<MemorySegment> get(MemorySegment key) {
        return map.get(key);
    }

    @Override
    public Iterator<Entry<MemorySegment>> allFrom(MemorySegment from) {
        return map.tailMap(from).values().iterator();
    }

    @Override
    public Iterator<Entry<MemorySegment>> allTo(MemorySegment to) {
        return map.headMap(to).values().iterator();
    }

    @Override
    public Iterator<Entry<MemorySegment>> all() {
        return map.values().iterator();
    }

    @Override
    public void upsert(Entry<MemorySegment> entry) {
        if (entry == null) {
            throw new IllegalArgumentException("Entry cannot be null.");
        }
        map.put(entry.key(), entry);
    }
}
