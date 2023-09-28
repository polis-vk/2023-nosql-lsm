package ru.vk.itmo.test.mozzhevilov;

import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.Comparator;
import java.util.Iterator;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class InMemoryDao implements Dao<MemorySegment, Entry<MemorySegment>> {

    MemorySegmentComparator memorySegmentComparator = new MemorySegmentComparator();

    SortedMap<MemorySegment, Entry<MemorySegment>> innerMap = new ConcurrentSkipListMap<>(memorySegmentComparator);

    @Override
    public Iterator<Entry<MemorySegment>> get(MemorySegment from, MemorySegment to) {
        if (from != null && to != null) {
            return innerMap.subMap(from, to).values().iterator();
        }
        if (to != null) {
            return innerMap.headMap(to).values().iterator();
        }
        if (from != null) {
            return innerMap.tailMap(from).values().iterator();
        }
        return innerMap.values().iterator();
    }

    @Override
    public Entry<MemorySegment> get(MemorySegment key) {
        return innerMap.get(key);
    }

    @Override
    public void upsert(Entry<MemorySegment> entry) {
        innerMap.put(entry.key(), entry);
    }

    static class MemorySegmentComparator implements Comparator<MemorySegment> {

        @Override
        public int compare(MemorySegment entry1, MemorySegment entry2) {
            var firstMismatch = entry1.mismatch(entry2);
            if (firstMismatch == -1) {
                return 0;
            }
            if (firstMismatch == entry1.byteSize()) {
                return -1;
            }
            if (firstMismatch == entry2.byteSize()) {
                return 1;
            }
            return Byte.compareUnsigned(
                    entry1.getAtIndex(ValueLayout.JAVA_BYTE, firstMismatch),
                    entry2.getAtIndex(ValueLayout.JAVA_BYTE, firstMismatch)
            );
        }

    }

}

