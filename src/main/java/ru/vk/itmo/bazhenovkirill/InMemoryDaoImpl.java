package ru.vk.itmo.bazhenovkirill;

import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;

import java.lang.foreign.MemorySegment;
import java.util.Iterator;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class InMemoryDaoImpl implements Dao<MemorySegment, Entry<MemorySegment>> {

    private final ConcurrentNavigableMap<MemorySegment, Entry<MemorySegment>> storage
            = new ConcurrentSkipListMap<>(MemorySegmentComparator.getInstance());

    @Override
    public Iterator<Entry<MemorySegment>> get(MemorySegment from, MemorySegment to) {
        if (from == null) {
            if (to != null) {
                return storage.headMap(to).values().iterator();
            }
            return storage.values().iterator();
        } else {
            if (to == null) {
                return storage.tailMap(from).values().iterator();
            }
            return storage.subMap(from, true, to, false).values().iterator();
        }
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
