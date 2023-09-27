package ru.vk.itmo.tyapuevdmitrij;

import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.Comparator;
import java.util.Iterator;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class InMemoryDao implements Dao<MemorySegment, Entry<MemorySegment>> {
    private final Comparator<MemorySegment> memorySegmentComparator = (segment1, segment2) -> {
        long offset = segment1.mismatch(segment2);
        if (offset == -1) {
            return 0;
        }
        if (offset == segment1.byteSize()) {
            return -1;
        }
        if (offset == segment2.byteSize()) {
            return 1;
        }
        return segment1.get(ValueLayout.JAVA_BYTE, offset) - segment2.get(ValueLayout.JAVA_BYTE, offset);
    };
    private final ConcurrentNavigableMap<MemorySegment, Entry<MemorySegment>> dataMap
            = new ConcurrentSkipListMap<>(memorySegmentComparator);

    @Override
    public Iterator<Entry<MemorySegment>> get(MemorySegment from, MemorySegment to) {
        if (from == null && to == null) {
            return dataMap.values().iterator();
        } else if (from == null) {
            return dataMap.headMap(to).values().iterator();
        } else if (to == null) {
            return dataMap.tailMap(from).values().iterator();
        }
        return dataMap.subMap(from, to).values().iterator();
    }

    @Override
    public Entry<MemorySegment> get(MemorySegment key) {
        return dataMap.get(key);
    }

    @Override
    public void upsert(Entry<MemorySegment> entry) {
        dataMap.put(entry.key(), entry);
    }
}


