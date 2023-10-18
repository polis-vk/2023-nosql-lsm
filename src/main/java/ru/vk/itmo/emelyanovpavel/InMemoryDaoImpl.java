package ru.vk.itmo.emelyanovpavel;

import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;

import java.lang.foreign.MemorySegment;
import java.util.Iterator;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class InMemoryDaoImpl implements Dao<MemorySegment, Entry<MemorySegment>> {

    private final ConcurrentNavigableMap<MemorySegment, Entry<MemorySegment>> storage;

    public InMemoryDaoImpl() {
        storage = new ConcurrentSkipListMap<>(new MemorySegmentComparator());
    }

    @Override
    public Iterator<Entry<MemorySegment>> get(MemorySegment from, MemorySegment to) {
        if (from == null && to == null) {
            return all();
        }
        if (from == null) {
            return allTo(to);
        }
        if (to == null) {
            return allFrom(from);
        }
        return storage.subMap(from, to)
                .values()
                .iterator();
    }

    @Override
    public Iterator<Entry<MemorySegment>> allFrom(MemorySegment from) {
        return storage.tailMap(from)
                .values()
                .iterator();
    }

    @Override
    public Iterator<Entry<MemorySegment>> allTo(MemorySegment to) {
        return storage.headMap(to)
                .values()
                .iterator();
    }

    @Override
    public Iterator<Entry<MemorySegment>> all() {
        return storage.values()
                .iterator();
    }

    @Override
    public void upsert(Entry<MemorySegment> entry) {
        storage.put(entry.key(), entry);
    }

    @Override
    public Entry<MemorySegment> get(MemorySegment key) {
        return storage.get(key);
    }

}
