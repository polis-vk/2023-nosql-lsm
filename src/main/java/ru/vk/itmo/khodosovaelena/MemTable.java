package ru.vk.itmo.khodosovaelena;

import ru.vk.itmo.BaseEntry;
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
    private final NavigableMap<MemorySegment, EntryWithTimestamp<MemorySegment>> map =
            new ConcurrentSkipListMap<>(
                    MemorySegmentComparator.INSTANCE);

    boolean isEmpty() {
        return map.isEmpty();
    }

    Iterator<EntryWithTimestamp<MemorySegment>> get(
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

    EntryWithTimestamp<MemorySegment> get(final MemorySegment key) {
        return map.get(key);
       // EntryWithTimestamp<MemorySegment> entryWithTimestamp = map.get(key);
       // return new BaseEntry<>(entryWithTimestamp.key(), entryWithTimestamp.value());
    }

    EntryWithTimestamp<MemorySegment> upsert(final Entry<MemorySegment> entry) {
        EntryWithTimestamp<MemorySegment> entryWithTimestamp =
                new BaseEntryWithTimestamp<>(entry.key(), entry.value(), System.currentTimeMillis());
        return map.put(entry.key(), entryWithTimestamp);
    }
}
