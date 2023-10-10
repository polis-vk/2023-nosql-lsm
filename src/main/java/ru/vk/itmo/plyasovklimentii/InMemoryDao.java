package ru.vk.itmo.plyasovklimentii;

import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;

import java.lang.foreign.MemorySegment;
import java.util.Iterator;
import java.util.concurrent.ConcurrentSkipListMap;

public class InMemoryDao implements Dao<MemorySegment, Entry<MemorySegment>> {

    private final ConcurrentSkipListMap<MemorySegment, Entry<MemorySegment>> storage;

    public InMemoryDao() {
        this.storage = new ConcurrentSkipListMap<>(new MemoryComparator());
    }

    @Override
    public Entry<MemorySegment> get(MemorySegment key) {
        return storage.get(key);
    }

    @Override
    public Iterator<Entry<MemorySegment>> get(MemorySegment from, MemorySegment to) {
        if (from == null && to == null) {
            return storage.sequencedValues().iterator();
        } else if (from == null) {
            return storage.headMap(to, false).sequencedValues().iterator();
        } else if (to == null) {
            return storage.tailMap(from, true).sequencedValues().iterator();
        } else {
            return storage.subMap(from,true, to, false).sequencedValues().iterator();
        }
    }

    @Override
    public void upsert(Entry<MemorySegment> entry) {
        storage.put(entry.key(), entry);
    }
}
