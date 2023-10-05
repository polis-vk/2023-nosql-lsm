package ru.vk.itmo.test.kononovvladimir;

import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.Comparator;
import java.util.Iterator;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

class InMemoryDao implements Dao<MemorySegment, Entry<MemorySegment>> {

    private final Comparator<MemorySegment> memorySegmentComparator = (o1, o2) -> {
        long mismatch = o1.mismatch(o2);
        if (mismatch == -1) return 0;
        else if (mismatch == o1.byteSize()) {
            return -1;
        } else if (mismatch == o2.byteSize()) {
            return 1;
        } else {
            byte b1 = o1.get(ValueLayout.JAVA_BYTE, mismatch);
            byte b2 = o2.get(ValueLayout.JAVA_BYTE, mismatch);
            return Byte.compare(b1, b2);
        }
    };
    private final ConcurrentNavigableMap<MemorySegment, Entry<MemorySegment>> concurrentSkipListMap
            = new ConcurrentSkipListMap<>(memorySegmentComparator);

    @Override
    public Entry<MemorySegment> get(MemorySegment key) {
        return concurrentSkipListMap.get(key);
    }

    @Override
    public Iterator<Entry<MemorySegment>> get(MemorySegment from, MemorySegment to) {
        if (from == null && to == null) {
            return concurrentSkipListMap.values().iterator();
        }

        if (from == null) {
            return concurrentSkipListMap.headMap(to).values().iterator();
        }

        if (to == null) {
            return concurrentSkipListMap.tailMap(from).values().iterator();
        }

        return concurrentSkipListMap.subMap(from, to).values().iterator();
    }

    @Override
    public void upsert(Entry<MemorySegment> entry) {
        if (entry == null || entry.key() == null || entry.value() == null) return;

        concurrentSkipListMap.put(entry.key(), entry);
    }
}
