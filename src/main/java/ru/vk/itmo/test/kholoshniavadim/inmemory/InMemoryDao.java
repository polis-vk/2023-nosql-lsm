package ru.vk.itmo.test.kholoshniavadim.inmemory;

import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;
import ru.vk.itmo.test.kholoshniavadim.utils.MemorySegmentComparator;

import java.lang.foreign.MemorySegment;
import java.util.Iterator;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

public final class InMemoryDao implements Dao<MemorySegment, Entry<MemorySegment>> {
    private final NavigableMap<MemorySegment, Entry<MemorySegment>> data =
            new ConcurrentSkipListMap<>(new MemorySegmentComparator());

    @Override
    public Iterator<Entry<MemorySegment>> get(MemorySegment from, MemorySegment to) {
        if (from == null && to == null) {
            return data.values().iterator();
        }

        if (from == null) {
            return data.headMap(to).values().iterator();
        }

        if (to == null) {
            return data.tailMap(from).values().iterator();
        }

        return data.subMap(from, to).values().iterator();
    }

    @Override
    public Entry<MemorySegment> get(MemorySegment key) {
        return data.get(key);
    }

    @Override
    public void upsert(Entry<MemorySegment> entry) {
        data.put(entry.key(), entry);
    }
}
