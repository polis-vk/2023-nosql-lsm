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
        long offset = left.mismatch(right);
        if (offset == -1) {
            return Long.compare(left.byteSize(), right.byteSize());
        } else if (offset == left.byteSize() || offset == right.byteSize()) {
            return offset == left.byteSize() ? -1 : 1;
        } else {
            return Byte.compare(getByte(left, offset), getByte(right, offset));
        }
    };

    private static byte getByte(MemorySegment memorySegment, long offset) {
        return memorySegment.get(ValueLayout.OfByte.JAVA_BYTE, offset);
    }

    @Override
    public Iterator<Entry<MemorySegment>> get(MemorySegment from, MemorySegment to) {
        ConcurrentNavigableMap<MemorySegment, Entry<MemorySegment>> subMap;
        if (from == null && to == null) {
            subMap = daoMap;
        } else if (from == null) {
            subMap = daoMap.headMap(to);
        } else if (to == null) {
            subMap = daoMap.tailMap(from);
        } else {
            subMap = daoMap.subMap(from, to);
        }
        return subMap.values().iterator();
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
        if (key == null) {
            return null;
        }
        return daoMap.get(key);
    }
}
