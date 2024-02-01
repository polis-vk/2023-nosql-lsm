package ru.vk.itmo.reference;

import ru.vk.itmo.Entry;

import java.lang.foreign.MemorySegment;
import java.util.Iterator;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * Memory table.
 *
 * @author incubos
 */
final class MemTable {
    private final NavigableMap<MemorySegment, TimeStampEntry> map =
            new ConcurrentSkipListMap<>(
                    MemorySegmentComparator.INSTANCE);

    boolean isEmpty() {
        return map.isEmpty();
    }

    Iterator<TimeStampEntry> get(
            final MemorySegment from,
            final MemorySegment to) {
        if (from == null && to == null) {
            // All
            return map.values().iterator();
        } else if (from == null) {
            // Head
            return map.headMap(to).values().iterator();
        } else if (to == null) {
            // Tail
            return map.tailMap(from).values().iterator();
        } else {
            // Slice
            return map.subMap(from, to).values().iterator();
        }
    }

    /// Тут мы пока просто игнорим таймсетмп
    Entry<MemorySegment> get(final MemorySegment key) {
        TimeStampEntry timeStampEntry = map.get(key);

        if (timeStampEntry != null) {
            return timeStampEntry.getClearEntry();
        }

        return null;
    }

    Entry<MemorySegment> upsert(final Entry<MemorySegment> entry) {
        return map.put(entry.key(), new TimeStampEntry(entry));
    }
}
