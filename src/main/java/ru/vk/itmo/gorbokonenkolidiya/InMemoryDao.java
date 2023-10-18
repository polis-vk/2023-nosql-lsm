package ru.vk.itmo.gorbokonenkolidiya;

import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.Iterator;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class InMemoryDao implements Dao<MemorySegment, Entry<MemorySegment>> {

    private final ConcurrentNavigableMap<MemorySegment, Entry<MemorySegment>> db;

    public InMemoryDao() {
        db = new ConcurrentSkipListMap<>(InMemoryDao::compare);
    }

    @Override
    public Iterator<Entry<MemorySegment>> get(MemorySegment from, MemorySegment to) {
        if (from == null && to == null) {
            return db.values().iterator();
        }
        if (from != null) {
            if (to != null) {
                return db.subMap(from, to).values().iterator();
            }
            return db.tailMap(from, true).values().iterator();
        }

        return db.headMap(to, false).values().iterator();
    }

    @Override
    public Entry<MemorySegment> get(MemorySegment key) {
        return db.get(key);
    }

    @Override
    public void upsert(Entry<MemorySegment> entry) {
        db.put(entry.key(), entry);
    }

    private static int compare(MemorySegment first, MemorySegment second) {
        long offset = MemorySegment.mismatch(first, 0, first.byteSize(), second, 0, second.byteSize());

        // Equal segments
        if (offset == -1) {
            return 0;
        }

        if (offset == first.byteSize()) {
            return -1;
        } else if (offset == second.byteSize()) {
            return 1;
        }

        var firstByteToCompare = first.get(ValueLayout.JAVA_BYTE, offset);
        var secondByteToCompare = second.get(ValueLayout.JAVA_BYTE, offset);

        return Byte.compare(firstByteToCompare, secondByteToCompare);
    }
}
