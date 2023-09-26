package ru.vk.itmo.shemetovalexey;

import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.Comparator;
import java.util.Iterator;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class InMemoryDao implements Dao<MemorySegment, Entry<MemorySegment>> {
    private final ConcurrentNavigableMap<MemorySegment, Entry<MemorySegment>> daoMap =
            new ConcurrentSkipListMap<>(comparator);
    private static final Comparator<MemorySegment> comparator = (left, right) -> {
        for (long i = 0; i < left.byteSize() && i < right.byteSize(); i++) {
            if (getByte(left, i) - getByte(right, i) != 0) {
                return getByte(left, i) - getByte(right, i);
            }
        }
        return Long.compare(left.byteSize(), right.byteSize());
    };

    private static byte getByte(MemorySegment memorySegment, long offset) {
        return memorySegment.get(ValueLayout.OfByte.JAVA_BYTE, offset);
    }

    @Override
    public Iterator<Entry<MemorySegment>> get(MemorySegment from, MemorySegment to) {
        if (from == null && to == null) {
            return daoMap.values().iterator();
        }
        if (from == null) {
            return daoMap.headMap(to).values().iterator();
        }
        if (to == null) {
            return daoMap.tailMap(from).values().iterator();
        }
        return daoMap.subMap(from, to).values().iterator();
    }

    @Override
    public void upsert(Entry<MemorySegment> entry) {
        if (entry == null) {
            return;
        }
        daoMap.put(entry.key(), entry);
    }

    @Override
    public Entry<MemorySegment> get(MemorySegment key) {
        return daoMap.get(key);
    }
}
