package ru.vk.itmo.podobaalexandr;

import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.Comparator;
import java.util.Iterator;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class InMemoryDaoImpl implements Dao<MemorySegment, Entry<MemorySegment>> {
    ConcurrentNavigableMap<MemorySegment, Entry<MemorySegment>> memorySegmentEntryMap
            = new ConcurrentSkipListMap<>(new MyComparator());

    @Override
    public Iterator<Entry<MemorySegment>> get(MemorySegment from, MemorySegment to) {
        ConcurrentNavigableMap<MemorySegment, Entry<MemorySegment>> innerMap = memorySegmentEntryMap;

        if (from != null && to == null) {
            innerMap = innerMap.tailMap(from);
        } else if (from == null && to != null) {
            innerMap = innerMap.headMap(to);
        } else if (from != null) {
            innerMap = innerMap.subMap(from, to);
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

            int sizeDiff = Long.compare(o1.byteSize(), o2.byteSize());
            long mismatch = o1.mismatch(o2);

            if (o1.byteSize() != 0 && o2.byteSize() != 0) {
                int diff = mismatch == -1
                        ? 0
                        : o1.get(ValueLayout.JAVA_BYTE, mismatch) - o2.get(ValueLayout.JAVA_BYTE, mismatch);
                return diff == 0 ? sizeDiff : diff;
            }

            return sizeDiff;
        }
    }

}
