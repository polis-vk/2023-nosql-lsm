package ru.vk.itmo.reshetnikovaleksei;

import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.Iterator;
import java.util.concurrent.ConcurrentSkipListMap;

public class InMemoryDao implements Dao<MemorySegment, Entry<MemorySegment>> {
    private final ConcurrentSkipListMap<MemorySegment, Entry<MemorySegment>> map;

    public InMemoryDao() {
        this.map = new ConcurrentSkipListMap<>((a, b) -> {
            var offset = a.mismatch(b);

            if (offset == -1) {
                return 0;
            } else if (offset == a.byteSize()) {
                return -1;
            } else if (offset == b.byteSize()) {
                return 1;
            } else {
                return Byte.compare(
                        a.get(ValueLayout.JAVA_BYTE, offset),
                        b.get(ValueLayout.JAVA_BYTE, offset)
                );
            }
        });
    }

    @Override
    public Iterator<Entry<MemorySegment>> get(MemorySegment from, MemorySegment to) {
        if (from == null && to == null) {
            return map.values().iterator();
        }

        if (from == null) {
            return map.headMap(to).values().iterator();
        }
        if (to == null) {
            return map.tailMap(from).values().iterator();
        }

        return map.subMap(from, to).values().iterator();
    }

    @Override
    public Entry<MemorySegment> get(MemorySegment key) {
        return map.get(key);
    }

    @Override
    public void upsert(Entry<MemorySegment> entry) {
        map.put(entry.key(), entry);
    }
}
