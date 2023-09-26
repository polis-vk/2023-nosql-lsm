package ru.vk.itmo.podobaalexandr;

import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;

import java.lang.foreign.MemorySegment;
import java.util.Comparator;
import java.util.Iterator;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class InMemoryDaoImpl implements Dao<MemorySegment, Entry<MemorySegment>> {
    ConcurrentNavigableMap<MemorySegment, Entry<MemorySegment>> memorySegmentEntryMap = new ConcurrentSkipListMap<>(new MyComparator());

    @Override
    public Iterator<Entry<MemorySegment>> get(MemorySegment from, MemorySegment to) {
        ConcurrentNavigableMap<MemorySegment, Entry<MemorySegment>> innerMap = memorySegmentEntryMap;
        if (from != null) {
            innerMap = innerMap.tailMap(from);
        }
        if (to != null) {
            innerMap = innerMap.headMap(to);
        }
        return innerMap.values().iterator();
    }

    @Override
    public void upsert(Entry<MemorySegment> entry) {
        memorySegmentEntryMap.put(entry.key(), entry);
    }

    @Override
    public Entry<MemorySegment> get(MemorySegment key) {
        return memorySegmentEntryMap.get(key);
    }

    private static class MyComparator implements Comparator<MemorySegment> {

        @Override
        public int compare(MemorySegment o1, MemorySegment o2) {
            return o1.asByteBuffer().compareTo(o2.asByteBuffer());
        }
    }

}
