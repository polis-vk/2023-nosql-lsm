package ru.vk.itmo.korzhvladislav;

import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.Comparator;
import java.util.Iterator;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class InMemoryDao implements Dao<MemorySegment, Entry<MemorySegment>> {
    private final Comparator<MemorySegment> compareMemorySegments = (segFirst, segSecond) -> {
        long offSetMismatch = segFirst.mismatch(segSecond);
        if (offSetMismatch == -1) {
            return 0;
        } else if (offSetMismatch == segFirst.byteSize()) {
            return -1;
        } else if (offSetMismatch == segSecond.byteSize()) {
            return 1;
        }
        return Byte.compare(segFirst.getAtIndex(ValueLayout.JAVA_BYTE, offSetMismatch),
                segSecond.getAtIndex(ValueLayout.JAVA_BYTE, offSetMismatch));
    };

    private final ConcurrentNavigableMap<MemorySegment, Entry<MemorySegment>> dataStore =
            new ConcurrentSkipListMap<>(compareMemorySegments);

    @Override
    public Entry<MemorySegment> get(MemorySegment key) {
        return dataStore.get(key);
    }

    @Override
    public Iterator<Entry<MemorySegment>> get(MemorySegment from, MemorySegment to) {
        if (from == null && to == null) {
            return dataStore.values().iterator();
        } else if (from == null) {
            return dataStore.headMap(to, false).values().iterator();
        } else if (to == null) {
            return dataStore.tailMap(from, true).values().iterator();
        } else {
            return dataStore.subMap(from, to).values().iterator();
        }
    }

    @Override
    public void upsert(Entry<MemorySegment> entry) {
        dataStore.put(entry.key(), entry);
    }
}
