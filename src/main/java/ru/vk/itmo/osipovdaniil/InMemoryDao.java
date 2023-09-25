package ru.vk.itmo.osipovdaniil;

import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.Comparator;
import java.util.Iterator;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class InMemoryDao implements Dao<MemorySegment, Entry<MemorySegment>> {

    private final ConcurrentNavigableMap<MemorySegment, Entry<MemorySegment>> memorySegmentMap
            = new ConcurrentSkipListMap<>(new MemorySegmentComparator());

    private static final class MemorySegmentComparator implements Comparator<MemorySegment> {

        @Override
        public int compare(final MemorySegment a, final MemorySegment b) {
            if (a.byteSize() < b.byteSize()) {
                return -1;
            } else if (a.byteSize() > b.byteSize()) {
                return 1;
            }
            long mismatchOffset = a.mismatch(b);
            if (mismatchOffset == -1) {
                return 0;
            }
            return Byte.compare(a.get(ValueLayout.JAVA_BYTE, mismatchOffset),
                    b.get(ValueLayout.JAVA_BYTE, mismatchOffset));
        }
    }

    /**
     * Returns ordered iterator of entries with keys between from (inclusive) and to (exclusive).
     *
     * @param from lower bound of range (inclusive)
     * @param to   upper bound of range (exclusive)
     * @return entries [from;to)
     */
    @Override
    public Iterator<Entry<MemorySegment>> get(final MemorySegment from, final MemorySegment to) {
        if (from == null && to == null) {
            return memorySegmentMap.values().iterator();
        } else if (from == null) {
            return memorySegmentMap.headMap(to).values().iterator();
        } else if (to == null) {
            return memorySegmentMap.tailMap(from).values().iterator();
        } else {
            return memorySegmentMap.subMap(from, to).values().iterator();
        }
    }

    /**
     * Returns entry by key. Note: default implementation is far from optimal.
     *
     * @param key entry`s key
     * @return entry
     */
    @Override
    public Entry<MemorySegment> get(final MemorySegment key) {
        return memorySegmentMap.get(key);
    }

    /**
     * Inserts of replaces entry.
     *
     * @param entry element to upsert
     */
    @Override
    public void upsert(final Entry<MemorySegment> entry) {
        memorySegmentMap.put(entry.key(), entry);
    }
}
