package ru.vk.itmo.tuzikovalexandr;

import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;

import java.lang.foreign.MemorySegment;
import java.util.Iterator;
import java.util.concurrent.ConcurrentSkipListMap;

public class InMemoryDaoImpl implements Dao<MemorySegment, Entry<MemorySegment>> {
    private final ConcurrentSkipListMap<MemorySegment, Entry<MemorySegment>> memory =
            new ConcurrentSkipListMap<>(new MemorySegmentComparator());

    @Override
    public Iterator<Entry<MemorySegment>> get(MemorySegment from, MemorySegment to) {

        if (from == null && to == null) {
            return memory.values().iterator();
        } else if (from == null) {
            return memory.headMap(to, false).values().iterator();
        } else if (to == null) {
            return memory.subMap(from, true, memory.lastKey(), false).values().iterator();
        } else {
            return memory.subMap(from, true, to, false).values().iterator();
        }
    }

    @Override
    public Entry<MemorySegment> get(MemorySegment key) {
        return memory.get(key);
    }

    @Override
    public void upsert(Entry<MemorySegment> entry) {
        memory.put(entry.key(), entry);
    }

    @Override
    public Iterator<Entry<MemorySegment>> all() {
        return memory.values().iterator();
    }
}
