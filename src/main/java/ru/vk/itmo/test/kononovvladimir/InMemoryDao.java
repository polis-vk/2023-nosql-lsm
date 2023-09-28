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

    public static Comparator<MemorySegment> memorySegmentComparator = (o1, o2) -> {
        long size1 = o1.byteSize();
        long size2 = o2.byteSize();
        int i1 = 0;
        int i2 = 0;
        int compare = Long.compare(size1, size2);

        while (i1 != size1 && i2 != size2) {
            compare = Byte.compare(o1.get(ValueLayout.JAVA_BYTE, i1), o2.get(ValueLayout.JAVA_BYTE, i2));
            if (compare != 0) {
                return compare;
            }
            i1++;
            i2++;
        }

        return compare;
    };
    ConcurrentNavigableMap<MemorySegment, Entry<MemorySegment>> concurrentSkipListMap
            = new ConcurrentSkipListMap<>(memorySegmentComparator);

    @Override
    public Entry<MemorySegment> get(MemorySegment key) {
        return concurrentSkipListMap.get(key);
    }

    @Override
    public Iterator<Entry<MemorySegment>> get(MemorySegment from, MemorySegment to) {
        MemorySegment newFrom = (from == null) ? MemorySegment.NULL : from;

        if (to == null) {
            return concurrentSkipListMap.tailMap(newFrom).values().iterator();
        }

        return concurrentSkipListMap.subMap(newFrom, to).values().iterator();
    }

    @Override
    public void upsert(Entry<MemorySegment> entry) {
        if (entry == null || entry.key() == null || entry.value() == null) return;

        concurrentSkipListMap.put(entry.key(), entry);
    }
}
