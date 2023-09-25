package ru.vk.itmo.test.kachmareugene;

import ru.vk.itmo.BaseEntry;
import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.*;
import java.util.concurrent.ConcurrentSkipListMap;

public class InMemoryDao implements Dao<MemorySegment, Entry<MemorySegment>> {

    private final SortedMap<MemorySegment, MemorySegment> mp;
    public InMemoryDao() {
        this.mp = new ConcurrentSkipListMap<>(new MemorySegmentComparator());
    }

    @Override
    public Iterator<Entry<MemorySegment>> get(MemorySegment from, MemorySegment to) {
        Iterator<Map.Entry<MemorySegment, MemorySegment>> iterator = null;

        if (from == null && to == null) {
            iterator = mp.entrySet().iterator();
        }
        if (from == null && to != null) {
            iterator = mp.headMap(to).entrySet().iterator();
        }

        if (from != null && to == null) {
            iterator = mp.tailMap(from).entrySet().iterator();
        }
        if (from != null && to != null) {
            iterator = mp.subMap(from, to).entrySet().iterator();
        }


        return new Iterator<Entry<MemorySegment>>() {
            Iterator<Map.Entry<MemorySegment, MemorySegment>> iterator;
            public Iterator<Entry<MemorySegment>> init(Iterator<Map.Entry<MemorySegment, MemorySegment>> it) {
                this.iterator = it;
                return this;
            }

            @Override
            public boolean hasNext() {
                return iterator.hasNext();
            }

            @Override
            public Entry<MemorySegment> next() {
                var rawEntry = iterator.next();
                return new BaseEntry<>(rawEntry.getKey(), rawEntry.getValue());
            }
        }.init(iterator);
    }


    @Override
    public Entry<MemorySegment> get(MemorySegment key) {
        return new BaseEntry<>(key, mp.get(key));
    }

    @Override
    public void upsert(Entry<MemorySegment> entry) {
        mp.put(entry.key(), entry.value());
    }

    private static class MemorySegmentComparator implements Comparator<MemorySegment> {

        @Override
        public int compare(MemorySegment segment1, MemorySegment segment2) {
            if (segment1.byteSize() != segment2.byteSize()) {
                return Long.compare(segment1.byteSize(), segment2.byteSize());
            }

            long firstDiffByte = segment1.mismatch(segment2);

            if (firstDiffByte == -1) {
                return 0;
            }
            return Byte.compare(segment1.get(ValueLayout.JAVA_BYTE, firstDiffByte),
                    segment2.get(ValueLayout.JAVA_BYTE, firstDiffByte));
        }
    }
}
