package ru.vk.itmo.test.kachmareugene;

import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Map;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class InMemoryDao implements Dao<MemorySegment, Entry<MemorySegment>> {

    private final SortedMap<MemorySegment, Entry<MemorySegment>> mp;
    public InMemoryDao() {
        this.mp = new ConcurrentSkipListMap<>(new MemorySegmentComparator());
    }
    @Override
    public Iterator<Entry<MemorySegment>> get(MemorySegment from, MemorySegment to) {
        SortedMap<MemorySegment, Entry<MemorySegment>> dataSlice;

        if (from == null && to == null) {
            dataSlice = mp;
        } else if (from == null) {
            dataSlice = mp.headMap(to);
        } else if (to == null) {
            dataSlice = mp.tailMap(from);
        } else {
            dataSlice = mp.subMap(from, to);
        }

        return new Iterator<Entry<MemorySegment>>() {
            Iterator<Map.Entry<MemorySegment, Entry<MemorySegment>>> iterator;
            public Iterator<Entry<MemorySegment>> init(Iterator<Map.Entry<MemorySegment, Entry<MemorySegment>>> it) {
                this.iterator = it;
                return this;
            }

            @Override
            public boolean hasNext() {
                return iterator.hasNext();
            }

            @Override
            public Entry<MemorySegment> next() {
                return iterator.next().getValue();
            }
        }.init(dataSlice.entrySet().iterator());
    }

    @Override
    public Entry<MemorySegment> get(MemorySegment key) {
        return mp.get(key);
    }

    @Override
    public void upsert(Entry<MemorySegment> entry) {
        mp.put(entry.key(), entry);
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
