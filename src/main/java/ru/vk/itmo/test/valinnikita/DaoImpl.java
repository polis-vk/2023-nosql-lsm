package ru.vk.itmo.test.valinnikita;

import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;

import java.lang.foreign.MemorySegment;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.concurrent.ConcurrentSkipListMap;

public class DaoImpl implements Dao<MemorySegment, Entry<MemorySegment>> {

    private final ConcurrentSkipListMap<MemorySegment, Entry<MemorySegment>> storage
            = new ConcurrentSkipListMap<>(new MemorySegmentComparator());

    @Override
    public Iterator<Entry<MemorySegment>> all() {
        return storage.values().iterator();
    }

    @Override
    public Entry<MemorySegment> get(MemorySegment key) {
        return storage.get(key);
    }

    @Override
    public Iterator<Entry<MemorySegment>> get(MemorySegment from, MemorySegment to) {
        if (from == null && to == null) {
            return storage.values().iterator();
        } else if (from == null) {
            return storage.headMap(to, false).values().iterator();
        } else if (to == null) {
            MemorySegment lastKey = storage.lastKey();
            return storage.subMap(from, true, lastKey, false).values().iterator();
        } else {
            return storage.subMap(from, true, to, false).values().iterator();
        }
    }

    @Override
    public void upsert(Entry<MemorySegment> entry) {
        if (entry != null) {
            storage.put(entry.key(), entry);
        }
    }

    private static class MemorySegmentComparator implements Comparator<MemorySegment> {
        @Override
        public int compare(MemorySegment o1, MemorySegment o2) {
            return Arrays.compareUnsigned(o1.asByteBuffer().array(), o2.asByteBuffer().array());
        }
    }
}
