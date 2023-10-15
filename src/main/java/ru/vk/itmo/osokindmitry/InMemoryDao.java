package ru.vk.itmo.osokindmitry;

import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.Collections;
import java.util.Iterator;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class InMemoryDao implements Dao<MemorySegment, Entry<MemorySegment>> {

    private final ConcurrentNavigableMap<MemorySegment, Entry<MemorySegment>> storage
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
                byte b1 = segment1.get(ValueLayout.JAVA_BYTE, offset);
                byte b2 = segment2.get(ValueLayout.JAVA_BYTE, offset);
                return Byte.compare(b1, b2);
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

        if (from == null && to == null) {
            return storage.values().iterator();
        } else if (from == null) {
            return storage.headMap(to).values().iterator();
        } else if (to == null) {
            return storage.tailMap(from).values().iterator();
        }
        return storage.tailMap(from).headMap(to).values().iterator();
    }

    @Override
    public void upsert(Entry<MemorySegment> entry) {
        storage.put(entry.key(), entry);
    }

}
