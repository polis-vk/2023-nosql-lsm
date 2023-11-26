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
public class MemTable implements ReadableTable, WriteableTable {
    private final NavigableMap<MemorySegment, Entry<MemorySegment>> map =
            new ConcurrentSkipListMap<>(
                    MemorySegmentComparator.INSTANCE);

    @Override
    public Iterator<Entry<MemorySegment>> get(
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

    @Override
    public Entry<MemorySegment> get(final MemorySegment key) {
        return map.get(key);
    }

    @Override
    public void upsert(final Entry<MemorySegment> entry) {
        map.put(entry.key(), entry);
    }
}
