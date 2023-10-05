package ru.vk.itmo.seletskayaalisa;

import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;
import java.lang.foreign.MemorySegment;
import java.util.Iterator;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class InMemoryDao implements Dao<MemorySegment, Entry<MemorySegment>> {
    private final NavigableMap<MemorySegment, Entry<MemorySegment>> segmentsMap;

    public InMemoryDao() {
        segmentsMap = new ConcurrentSkipListMap<>(new MemorySegmentComparator());
    }

    @Override
    public Entry<MemorySegment> get(MemorySegment key) {
        return segmentsMap.get(key);
    }

    @Override
    public Iterator<Entry<MemorySegment>> get(MemorySegment from, MemorySegment to) {
        if (from == null && to == null) {
            return segmentsMap.values().iterator();
        }
        if (from == null) {
            return segmentsMap.headMap(to).values().iterator();
        }
        if (to == null) {
            return segmentsMap.tailMap(from).values().iterator();
        }
        return segmentsMap.subMap(from, to).values().iterator();
    }

    @Override
    public void upsert(Entry<MemorySegment> entry) {
        if (entry == null) {
            throw new IllegalArgumentException("The provided entry is null");
        }
        segmentsMap.put(entry.key(), entry);
    }

}
