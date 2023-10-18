package ru.vk.itmo.grunskiialexey;

import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.Comparator;
import java.util.Iterator;
import java.util.concurrent.ConcurrentSkipListMap;

public class MemorySegmentDao implements Dao<MemorySegment, Entry<MemorySegment>> {
    private final Comparator<MemorySegment> comparator = (o1, o2) -> {
        long firstMismatch = o1.mismatch(o2);
        if (firstMismatch == -1) {
            return 0;
        }
        if (firstMismatch == o1.byteSize()) {
            return -1;
        }
        if (firstMismatch == o2.byteSize()) {
            return 1;
        }

        byte byte1 = o1.get(ValueLayout.JAVA_BYTE, firstMismatch);
        byte byte2 = o2.get(ValueLayout.JAVA_BYTE, firstMismatch);
        return Byte.compare(byte1, byte2);
    };

    private final ConcurrentSkipListMap<MemorySegment, Entry<MemorySegment>> data =
            new ConcurrentSkipListMap<>(comparator);

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
        data.put(entry.key(), entry);
    }
}
