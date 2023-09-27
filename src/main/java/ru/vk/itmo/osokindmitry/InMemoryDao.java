package ru.vk.itmo.osokindmitry;

import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.Collections;
import java.util.Iterator;
import java.util.concurrent.ConcurrentSkipListMap;

public class InMemoryDao implements Dao<MemorySegment, Entry<MemorySegment>> {

    private final ConcurrentSkipListMap<MemorySegment, Entry<MemorySegment>> storage
            = new ConcurrentSkipListMap<>(
            (segment1, segment2) -> {
                long offset = segment1.mismatch(segment2);
                if (offset == -1) {
                    return 0;
                } else if (offset == segment1.byteSize()) {
                    return -1;
                } else if (offset == segment2.byteSize()) {
                    return 1;
                }
                return segment1.get(ValueLayout.JAVA_BYTE, offset) - segment2.get(ValueLayout.JAVA_BYTE, offset);
            }
    );

    @Override
    public Entry<MemorySegment> get(MemorySegment key) {
        return storage.get(key);
    }

    @Override
    public Iterator<Entry<MemorySegment>> get(MemorySegment from, MemorySegment to) {
        if (storage.isEmpty()) {
            return Collections.emptyIterator();
        }
        boolean empty = to == null;
        MemorySegment first = from == null ? storage.firstKey() : from;
        MemorySegment last = to == null ? storage.lastKey() : to;
        return storage.subMap(first, true, last, empty).values().iterator();
    }

    @Override
    public void upsert(Entry<MemorySegment> entry) {
        storage.put(entry.key(), entry);
    }

}
