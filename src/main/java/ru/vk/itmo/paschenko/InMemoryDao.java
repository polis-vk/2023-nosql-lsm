package ru.vk.itmo.paschenko;

import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.Comparator;
import java.util.Iterator;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class InMemoryDao implements Dao<MemorySegment, Entry<MemorySegment>> {
    private final Comparator<MemorySegment> comparator = InMemoryDao::compareMemorySegments;
    private final NavigableMap<MemorySegment, Entry<MemorySegment>> storage = new ConcurrentSkipListMap<>(comparator);

    private static int compareMemorySegments(MemorySegment o1, MemorySegment o2) {
        long mismatch = o1.mismatch(o2);
        if (mismatch == -1) {
            return 0;
        }
        if (mismatch == o1.byteSize()) {
            return -1;
        }
        if (mismatch == o2.byteSize()) {
            return 1;
        }

        byte b1 = o1.get(ValueLayout.JAVA_BYTE, mismatch);
        byte b2 = o2.get(ValueLayout.JAVA_BYTE, mismatch);
        return Byte.compare(b1, b2);
    }

    @Override
    public Iterator<Entry<MemorySegment>> get(MemorySegment from, MemorySegment to) {
        if (from == null && to == null) {
            return storage.values().iterator();
        }
        if (from == null) {
            return storage.headMap(to).values().iterator();
        }
        if (to == null) {
            return storage.tailMap(from).values().iterator();
        }
        return storage.subMap(from, to).values().iterator();
    }

    @Override
    public Entry<MemorySegment> get(MemorySegment key) {
        return storage.get(key);
    }

    @Override
    public void upsert(Entry<MemorySegment> entry) {
        storage.put(entry.key(), entry);
    }
}
