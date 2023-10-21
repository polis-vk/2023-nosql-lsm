package ru.vk.itmo.bandurinvladislav;

import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.Comparator;
import java.util.Iterator;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class InMemoryDao implements Dao<MemorySegment, Entry<MemorySegment>> {
    private static final Comparator<MemorySegment> MEMORY_SEGMENT_COMPARATOR = (m1, m2) -> {
        long mismatch = m1.mismatch(m2);
        if (mismatch == m2.byteSize()) {
            return 1;
        } else if (mismatch == m1.byteSize()) {
            return -1;
        } else if (mismatch == -1) {
            return 0;
        } else {
            return m1.get(ValueLayout.JAVA_BYTE, mismatch) - m2.get(ValueLayout.JAVA_BYTE, mismatch);
        }
    };

    private final ConcurrentNavigableMap<MemorySegment, Entry<MemorySegment>> inMemoryStorage;

    public InMemoryDao() {
        inMemoryStorage = new ConcurrentSkipListMap<>(MEMORY_SEGMENT_COMPARATOR);
    }

    @Override
    public Iterator<Entry<MemorySegment>> get(MemorySegment from, MemorySegment to) {
        return getEntryIterator(from, to);
    }

    @Override
    public Entry<MemorySegment> get(MemorySegment key) {
        return inMemoryStorage.get(key);
    }

    @Override
    public void upsert(Entry<MemorySegment> entry) {
        inMemoryStorage.put(entry.key(), entry);
    }

    private Iterator<Entry<MemorySegment>> getEntryIterator(MemorySegment from, MemorySegment to) {
        if (from == null && to == null) {
            return inMemoryStorage.values().iterator();
        } else if (from == null) {
            return inMemoryStorage.headMap(to, false).values().iterator();
        } else if (to == null) {
            return inMemoryStorage.tailMap(from, true).values().iterator();
        } else {
            return inMemoryStorage.subMap(from, true, to, false).values().iterator();
        }
    }
}
