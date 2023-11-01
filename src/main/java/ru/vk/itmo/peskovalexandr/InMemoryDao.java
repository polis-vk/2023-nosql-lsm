package ru.vk.itmo.peskovalexandr;

import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;
import java.lang.foreign.MemorySegment;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class InMemoryDao implements Dao<MemorySegment, Entry<MemorySegment>> {

    protected final ConcurrentNavigableMap<MemorySegment, Entry<MemorySegment>> entryMap =
            new ConcurrentSkipListMap<>(new MemorySegmentComparator());

    @Override
    public Entry<MemorySegment> get(MemorySegment key) {
        return entryMap.get(key);
    }

    @Override
    public Iterator<Entry<MemorySegment>> get(MemorySegment from, MemorySegment to) {
        return getSubMap(from, to).values().iterator();
    }

    @Override
    public void upsert(Entry<MemorySegment> entry) {
        entryMap.put(entry.key(), entry);
    }

    private Map<MemorySegment, Entry<MemorySegment>> getSubMap(MemorySegment from, MemorySegment to) {
        if (from == null && to == null) {
            return entryMap;
        }
        if (from == null) {
            return entryMap.headMap(to, false);
        }
        if (to == null) {
            return entryMap.tailMap(from, true);
        }
        return entryMap.subMap(from, true, to, false);
    }
}
