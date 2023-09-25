package ru.vk.itmo.smirnovdmitrii;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Map;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import ru.vk.itmo.BaseEntry;
import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;

public class InMemoryDao implements Dao<MemorySegment, Entry<MemorySegment>> {

    private static final class MemorySegmentComparator implements Comparator<MemorySegment> {
        @Override
        public int compare(final MemorySegment o1, final MemorySegment o2) {
            long offset = o1.mismatch(o2);
            if (offset == -1) {
                return 0;
            } else if (o1.byteSize() == offset) {
                return -1;
            } else if (o2.byteSize() == offset) {
                return 1;
            }
            return Byte.compare(o1.get(ValueLayout.JAVA_BYTE, offset), o2.get(ValueLayout.JAVA_BYTE, offset));
        }
    }

    private final NavigableMap<MemorySegment, MemorySegment> memorySegmentMap =
            new ConcurrentSkipListMap<>(new MemorySegmentComparator());

    @Override
    public Iterator<Entry<MemorySegment>> get(final MemorySegment from, final MemorySegment to) {

        class InMemoryIterator implements Iterator<Entry<MemorySegment>> {

            private final Iterator<Map.Entry<MemorySegment, MemorySegment>> mapIterator;

            public InMemoryIterator() {
                final Map<MemorySegment, MemorySegment> map;
                if (from == null && to == null) {
                    map = memorySegmentMap;
                } else if (from == null) {
                    map = memorySegmentMap.headMap(to);
                } else if (to == null) {
                    map = memorySegmentMap.tailMap(from);
                } else {
                    map = memorySegmentMap.subMap(from, to);
                }
                mapIterator = map.entrySet().iterator();
            }

            @Override
            public boolean hasNext() {
                return mapIterator.hasNext();
            }

            @Override
            public Entry<MemorySegment> next() {
                final Map.Entry<MemorySegment, MemorySegment> entry = mapIterator.next();
                return new BaseEntry<>(entry.getKey(), entry.getValue());
            }
        }

        return new InMemoryIterator();
    }

    @Override
    public Entry<MemorySegment> get(final MemorySegment key) {
        final MemorySegment value = memorySegmentMap.get(key);
        if (value == null) {
            return null;
        }
        return new BaseEntry<>(key, value);
    }

    @Override
    public void upsert(final Entry<MemorySegment> entry) {
        memorySegmentMap.put(entry.key(), entry.value());
    }
}
