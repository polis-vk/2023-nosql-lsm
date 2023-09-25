package ru.vk.itmo.novichkovandrew;

import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.Iterator;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class InMemoryDao implements Dao<MemorySegment, Entry<MemorySegment>> {
    /**
     * I used {@link ConcurrentSkipListMap} of (entry.key(), entry) here, because
     * method {@link #get(MemorySegment, MemorySegment)} takes
     * keys as arguments. <br>
     * {@link ConcurrentSkipListMap#subMap(Object, Object)} method return subMap by keys.
     * If we try to use
     * {@link java.util.concurrent.ConcurrentSkipListSet} in order to save
     * entries only, that in the method {@link #get(MemorySegment, MemorySegment)} we need to allocate new BaseEntry, cause
     * {@link java.util.concurrent.ConcurrentSkipListSet#subSet(Object, Object)} takes two entries.<br>
     * Example:
     * <pre> {@code
     * @Override
     * public Iterator<Entry<MemorySegment>> get(MemorySegment from, MemorySegment to) {
     *     Entry<MemorySegment> dummyFrom = new BaseEntry<>(from, null);
     *     Entry<MemorySegment> dummyFrom = new BaseEntry<>(from, null);
     *     return getSubSet(dummyFrom, dummyTo).iterator();
     * }}</pre>
     * What's realisation is better?
     */
    final ConcurrentSkipListMap<MemorySegment, Entry<MemorySegment>> entriesMap;

    public InMemoryDao() {
        this.entriesMap = new ConcurrentSkipListMap<>(this::compareMemorySegment);
    }

    private NavigableMap<MemorySegment, Entry<MemorySegment>> getSubMap(MemorySegment from, MemorySegment to) {
        if (from != null && to != null) {
            return entriesMap.subMap(from, to);
        } else if (from != null) {
            return entriesMap.tailMap(from, true);
        } else if (to != null) {
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
