package ru.vk.itmo.novichkovandrew;

import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.Iterator;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class InMemoryDao implements Dao<MemorySegment, Entry<MemorySegment>> {

    private final ConcurrentSkipListMap<MemorySegment, Entry<MemorySegment>> entriesMap;

    public InMemoryDao() {
        this.entriesMap = new ConcurrentSkipListMap<>(this::compareMemorySegment);
    }

    private NavigableMap<MemorySegment, Entry<MemorySegment>> getSubMap(MemorySegment from, MemorySegment to) {
        if (from != null && to != null) {
            return entriesMap.subMap(from, to);
        }
        if (from != null) {
            return entriesMap.tailMap(from, true);
        }
        if (to != null) {
            return entriesMap.headMap(to, false);
        }
        return entriesMap;
    }

    @Override
    public Iterator<Entry<MemorySegment>> get(MemorySegment from, MemorySegment to) {
        return getSubMap(from, to).values().iterator();
    }

    @Override
    public Entry<MemorySegment> get(MemorySegment key) {
        return entriesMap.get(key);
    }

    @Override
    public void upsert(Entry<MemorySegment> entry) {
        entriesMap.put(entry.key(), entry);
    }

    private int compareMemorySegment(MemorySegment first, MemorySegment second) {
        if (first == null || second == null) return -1;
        long missIndex = first.mismatch(second);
        if (missIndex == first.byteSize()) {
            return -1;
        }
        if (missIndex == second.byteSize()) {
            return 1;
        }
        return missIndex == -1 ? 0 : Byte.compare(
                first.getAtIndex(ValueLayout.JAVA_BYTE, missIndex),
                second.getAtIndex(ValueLayout.JAVA_BYTE, missIndex)
        );
    }
}
