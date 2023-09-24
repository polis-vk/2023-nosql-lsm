package ru.vk.itmo.prokopyevnikita;

import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.Iterator;
import java.util.concurrent.ConcurrentSkipListMap;

public class InMemoryDaoImpl implements Dao<MemorySegment, Entry<MemorySegment>> {
    private final ConcurrentSkipListMap<MemorySegment, Entry<MemorySegment>> map =
            new ConcurrentSkipListMap<>((o1, o2) -> {
                long relativeOffset = o1.mismatch(o2);
                if (relativeOffset == -1) {
                    return 0;
                } else if (relativeOffset == o1.byteSize()) {
                    return -1;
                } else if (relativeOffset == o2.byteSize()) {
                    return 1;
                }
                return Byte.compare(
                        o1.get(ValueLayout.JAVA_BYTE, relativeOffset),
                        o2.get(ValueLayout.JAVA_BYTE, relativeOffset)
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
            throw new IllegalArgumentException("Value cannot be null");
        }
        map.put(entry.key(), entry);
    }
}
