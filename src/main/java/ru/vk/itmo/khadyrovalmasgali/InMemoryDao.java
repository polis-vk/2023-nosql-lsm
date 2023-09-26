package ru.vk.itmo.khadyrovalmasgali;

import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.Iterator;
import java.util.concurrent.ConcurrentSkipListMap;

public class InMemoryDao implements Dao<MemorySegment, Entry<MemorySegment>> {

    private final ConcurrentSkipListMap<MemorySegment,
            Entry<MemorySegment>> data = new ConcurrentSkipListMap<>((o1, o2) -> {
        long size1 = o1.byteSize();
        long size2 = o2.byteSize();
        for (long i = 0, j = 0; i < size1 && j < size2; ++i, ++j) {
            byte b1 = o1.get(ValueLayout.JAVA_BYTE, i);
            byte b2 = o2.get(ValueLayout.JAVA_BYTE, j);
            if (b1 > b2) {
                return 1;
            }
            if (b1 < b2) {
                return -1;
            }
        }
        return Long.compare(size1, size2);
    });

    @Override
    public Iterator<Entry<MemorySegment>> get(MemorySegment from, MemorySegment to) {
        if (from == null && to == null) {
            return data.values().iterator();
        } else if (from == null) {
            return data.headMap(to).values().iterator();
        } else if (to == null) {
            return data.tailMap(from).values().iterator();
        } else {
            return data.subMap(from, to).values().iterator();
        }
    }

    @Override
    public Entry<MemorySegment> get(MemorySegment key) {
        return data.get(key);
    }

    @Override
    public void upsert(Entry<MemorySegment> entry) {
        if (entry != null) {
            data.put(entry.key(), entry);
        }
    }
}
