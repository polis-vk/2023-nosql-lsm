package ru.vk.itmo.dalbeevgeorgii;

import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.Iterator;
import java.util.concurrent.ConcurrentSkipListMap;

public class DaoImpl implements Dao<MemorySegment, Entry<MemorySegment>> {
    private final ConcurrentSkipListMap<MemorySegment, Entry<MemorySegment>> map = new ConcurrentSkipListMap<>(
            (segment1, segment2) -> {
                long offset = segment1.mismatch(segment2);
                if (offset == -1) {
                    return 0;
                } else if (offset == segment1.byteSize()) {
                    return -1;
                } else if (offset == segment2.byteSize()) {
                    return 1;
                }
                return Byte.compare(
                        segment1.get(ValueLayout.JAVA_BYTE, offset),
                        segment2.get(ValueLayout.JAVA_BYTE, offset)
                );
            });

    @Override
    public Iterator<Entry<MemorySegment>> get(MemorySegment from, MemorySegment to) {
        if (from == null && to == null) {
            return map.values().iterator();
        } else if (to == null) {
            return map.tailMap(from).values().iterator();
        } else if (from == null) {
            return map.headMap(to).values().iterator();
        }
        return map.subMap(from, to).values().iterator();
    }

    @Override
    public Entry<MemorySegment> get(MemorySegment key) {
        return map.get(key);
    }

    @Override
    public void upsert(Entry<MemorySegment> entry) {
        if (entry == null) {
            throw new IllegalArgumentException("Entry cannot be null");
        }
        map.put(entry.key(), entry);
    }
}
