package ru.vk.itmo.dyagayalexandra;

import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;

import java.lang.foreign.MemorySegment;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class InMemoryDao implements Dao<MemorySegment, Entry<MemorySegment>> {

    private final Comparator<MemorySegment> comparator = new MemorySegmentComparator();

    private final ConcurrentNavigableMap<MemorySegment, Entry<MemorySegment>> storage;

    public InMemoryDao() {
        storage = new ConcurrentSkipListMap<>(comparator);
    }

    @Override
    public Iterator<Entry<MemorySegment>> get(MemorySegment from, MemorySegment to) {
        Collection<Entry<MemorySegment>> values;
        if (from == null && to == null) {
            values = storage.values();
        } else if (from == null) {
            values = storage.headMap(to).values();
        } else if (to == null) {
            values = storage.tailMap(from).values();
        } else {
            values = storage.subMap(from, to).values();
        }

        return values.iterator();
    }

    @Override
    public Entry<MemorySegment> get(MemorySegment key) {
        return storage.get(key);
    }

    @Override
    public void upsert(Entry<MemorySegment> entry) {
        storage.put(entry.key(), entry);
    }
}
