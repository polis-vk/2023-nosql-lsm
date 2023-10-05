package ru.vk.itmo.khodosovaelena;

import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.Comparator;
import java.util.Iterator;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class InMemoryDao implements Dao<MemorySegment, Entry<MemorySegment>> {
    private final ConcurrentNavigableMap<MemorySegment, Entry<MemorySegment>> memorySegmentEntries
            = new ConcurrentSkipListMap<>(new MemorySegmentComparator());

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
        return memorySegmentEntries.subMap(from, to).values().iterator();
    }

    @Override
    public void upsert(Entry<MemorySegment> entry) {
        memorySegmentEntries.put(entry.key(), entry);
    }

    public static class MemorySegmentComparator implements Comparator<MemorySegment> {
        @Override
        public int compare(MemorySegment segment1, MemorySegment segment2) {
            long offset = segment1.mismatch(segment2);
            if (offset == -1) {
                return 0;
            } else if (offset == segment2.byteSize()) {
                return 1;
            } else if (offset == segment1.byteSize()) {
                return -1;
            }
            return Byte.compare(segment1.get(ValueLayout.JAVA_BYTE, offset),
                    segment2.get(ValueLayout.JAVA_BYTE, offset));
        }
    }
}
