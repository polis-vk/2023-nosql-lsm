package ru.vk.itmo.novichkovandrew;

import ru.vk.itmo.BaseEntry;
import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.*;
import java.util.concurrent.ConcurrentSkipListMap;

public class InMemoryDao implements Dao<MemorySegment, Entry<MemorySegment>> {

    final ConcurrentSkipListMap<MemorySegment, Entry<MemorySegment>> data;

    public InMemoryDao() {
        this.data = new ConcurrentSkipListMap<>(this::compareMemorySegment);
    }


    private Map<MemorySegment, Entry<MemorySegment>> getSubMap(MemorySegment from, MemorySegment to) {
        if (from != null && to != null) {
            return data.subMap(from, to);
        } else if (from != null) {
            return data.tailMap(from, true);
        } else if (to != null) {
            return data.headMap(to, false);
        }
        return data;
    }

    @Override
    public Iterator<Entry<MemorySegment>> get(MemorySegment from, MemorySegment to) {
        return getSubMap(from, to).values().iterator();
    }

    @Override
    public Entry<MemorySegment> get(MemorySegment key) {
        Iterator<Entry<MemorySegment>> iterator = get(key, null);
        if (!iterator.hasNext()) {
            return null;
        }
        Entry<MemorySegment> nextKey = iterator.next();
        if (key.mismatch(nextKey.key()) == -1) {
            return nextKey;
        }
        return null;
    }

    @Override
    public void upsert(Entry<MemorySegment> entry) {
        data.put(entry.key(), entry);
    }

    private int compareMemorySegment(MemorySegment first, MemorySegment second) {
        if (first == null || second == null) return -1;
        if (first.byteSize() != second.byteSize()) {
            return Long.compare(first.byteSize(), second.byteSize());
        }
        long missIndex = first.mismatch(second);
        return missIndex == -1 ? 0 : Byte.compare(
                first.getAtIndex(ValueLayout.JAVA_BYTE, missIndex),
                second.getAtIndex(ValueLayout.JAVA_BYTE, missIndex)
        );
    }
}
