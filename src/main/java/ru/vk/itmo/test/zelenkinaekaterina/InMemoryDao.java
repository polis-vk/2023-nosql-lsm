package ru.vk.itmo.test.zelenkinaekaterina;

import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.Comparator;
import java.util.Iterator;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class InMemoryDao implements Dao<MemorySegment, Entry<MemorySegment>> {
    private final SortedMap<MemorySegment, Entry<MemorySegment>> storage;

    public InMemoryDao() {
        storage = new ConcurrentSkipListMap<>(new MemorySegmentComparator());
    }

    @Override
    public Entry<MemorySegment> get(MemorySegment key) {
        return storage.get(key);
    }

    @Override
    public Iterator<Entry<MemorySegment>> get(MemorySegment from, MemorySegment to) {
        SortedMap<MemorySegment, Entry<MemorySegment>> subStorage;
        if (from == null && to == null) {
            subStorage = storage;
        } else if (from == null) {
            subStorage = storage.headMap(to);
        } else if (to == null) {
            subStorage = storage.tailMap(from);
        } else {
            subStorage = storage.subMap(from, to);
        }
        return subStorage.values().iterator();
    }

    @Override
    public void upsert(Entry<MemorySegment> entry) {
        storage.put(entry.key(), entry);
    }

    private static class MemorySegmentComparator implements Comparator<MemorySegment> {
        @Override
        public int compare(MemorySegment ms1, MemorySegment ms2) {
            long offset = ms1.mismatch(ms2);
            if (offset == -1) {
                return 0;
            }
            if (offset == ms1.byteSize()) {
                return -1;
            }
            if (offset == ms2.byteSize()) {
                return 1;
            }
            return Byte.compare(ms1.get(ValueLayout.JAVA_BYTE, offset), ms2.get(ValueLayout.JAVA_BYTE, offset));
        }
    }

}
