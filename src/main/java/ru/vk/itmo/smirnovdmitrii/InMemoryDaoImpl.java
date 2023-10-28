package ru.vk.itmo.smirnovdmitrii;

import ru.vk.itmo.Entry;
import ru.vk.itmo.smirnovdmitrii.util.MemorySegmentComparator;

import java.lang.foreign.MemorySegment;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class InMemoryDaoImpl implements InMemoryDao<MemorySegment, Entry<MemorySegment>> {
    private final SortedMap<MemorySegment, Entry<MemorySegment>> storage =
            new ConcurrentSkipListMap<>(new MemorySegmentComparator());

    @Override
    public Iterator<Entry<MemorySegment>> get(final MemorySegment from, final MemorySegment to) {
        final Map<MemorySegment, Entry<MemorySegment>> map;
        if (from == null && to == null) {
            map = storage;
        } else if (from == null) {
            map = storage.headMap(to);
        } else if (to == null) {
            map = storage.tailMap(from);
        } else {
            map = storage.subMap(from, to);
        }
        return map.values().iterator();
    }

    @Override
    public Entry<MemorySegment> get(final MemorySegment key) {
        return storage.get(key);
    }

    @Override
    public void upsert(final Entry<MemorySegment> entry) {
        storage.put(entry.key(), entry);
    }

    @Override
    public Iterable<Entry<MemorySegment>> commit() {
        final Iterable<Entry<MemorySegment>> result = new ArrayList<>(storage.values());
        storage.clear();
        return result;
    }

    @Override
    public void close() {
        storage.clear();
    }

}
