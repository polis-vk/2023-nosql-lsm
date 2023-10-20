package ru.vk.itmo.pologovnikita;

import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;

import java.lang.foreign.MemorySegment;
import java.util.Iterator;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class InMemoryDao implements Dao<MemorySegment, Entry<MemorySegment>> {
    ConcurrentNavigableMap<MemorySegment, Entry<MemorySegment>> segmentToEntry =
            new ConcurrentSkipListMap<>(new MemorySegmentComparator());

    @Override
    public Iterator<Entry<MemorySegment>> get(MemorySegment from, MemorySegment to) {
        if (from == null && to == null) {
            return segmentToEntry.values().iterator();
        }
        ConcurrentNavigableMap<MemorySegment, Entry<MemorySegment>> subMap;
        if (from == null) {
            subMap = segmentToEntry.headMap(to);
        } else if (to == null) {
            subMap = segmentToEntry.tailMap(from);
        } else {
            subMap = segmentToEntry.subMap(from, to);
        }
        return subMap.values().iterator();
    }

    @Override
    public Entry<MemorySegment> get(MemorySegment key) {
        return segmentToEntry.get(key);
    }

    @Override
    public void upsert(Entry<MemorySegment> entry) {
        segmentToEntry.put(entry.key(), entry);
    }
}
