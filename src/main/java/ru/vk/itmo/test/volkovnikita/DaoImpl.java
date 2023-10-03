package ru.vk.itmo.test.volkovnikita;

import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;

import java.lang.foreign.MemorySegment;
import java.util.Iterator;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class DaoImpl implements Dao<MemorySegment, Entry<MemorySegment>> {

    private final NavigableMap<MemorySegment, Entry<MemorySegment>> memorySegmentEntries;

    public DaoImpl() {
        MemorySegmentComparator msComparator = new MemorySegmentComparator();
        memorySegmentEntries = new ConcurrentSkipListMap<>(msComparator);
    }

    @Override
    public Entry<MemorySegment> get(MemorySegment key) {
        return memorySegmentEntries.get(key);
    }

    @Override
    public Iterator<Entry<MemorySegment>> get(MemorySegment from, MemorySegment to) {
        if (from == null && to == null) {
            return memorySegmentEntries.values().iterator();
        }
        if (from == null) {
            return memorySegmentEntries.headMap(to).values().iterator();
        }
        if (to == null) {
            return memorySegmentEntries.tailMap(from).values().iterator();
        }

        return memorySegmentEntries.subMap(from, true, to, false).values().iterator();
    }

    @Override
    public void upsert(Entry<MemorySegment> entry) {
        memorySegmentEntries.put(entry.key(), entry);
    }

    @Override
    public Iterator<Entry<MemorySegment>> all() {
        return memorySegmentEntries.values().iterator();
    }
}
