package ru.vk.itmo.trutnevsevastian;

import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Map;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class InMemoryDao implements Dao<MemorySegment, Entry<MemorySegment>> {

    private final NavigableMap<MemorySegment, Entry<MemorySegment>> storage =
            new ConcurrentSkipListMap<>(new MemorySegmentComparator());

    @Override
    public Iterator<Entry<MemorySegment>> get(MemorySegment from, MemorySegment to) {
        return subMap(from, to).values().iterator();
    }

    private Map<MemorySegment, Entry<MemorySegment>> subMap(MemorySegment from, MemorySegment to) {
        if (from == null && to == null) {
            return storage;
        }
        if (from == null) {
            return storage.headMap(to);
        }
        if (to == null) {
            return storage.tailMap(from, true);
        }

        return storage.subMap(from, true, to, false);
    }

    @Override
    public void upsert(Entry<MemorySegment> entry) {
        storage.put(entry.key(), entry);
    }

    @Override
    public Entry<MemorySegment> get(MemorySegment key) {
        return storage.get(key);
    }

    private static class MemorySegmentComparator implements Comparator<MemorySegment> {
        @Override
        public int compare(MemorySegment o1, MemorySegment o2) {
            long lim = Math.min(o1.byteSize(), o2.byteSize());
            long k = MemorySegment.mismatch(o1, 0, lim, o2, 0, lim);
            if (k >= 0) {
                return o1.get(ValueLayout.JAVA_BYTE, k) - o2.get(ValueLayout.JAVA_BYTE, k);
            }
            if (o1.byteSize() == o2.byteSize()) {
                return 0;
            }
            return o1.byteSize() < o2.byteSize() ? -1 : 1;
        }
    }
}
