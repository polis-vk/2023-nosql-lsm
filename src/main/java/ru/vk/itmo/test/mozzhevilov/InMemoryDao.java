package ru.vk.itmo.test.mozzhevilov;

import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.*;
import java.util.concurrent.ConcurrentSkipListMap;

public class InMemoryDao<D, E extends Entry<D>> implements Dao<MemorySegment, Entry<MemorySegment>> {

    MemorySegmentComparator memorySegmentComparator = new MemorySegmentComparator();

    SortedMap<MemorySegment, Entry<MemorySegment>> inner = new ConcurrentSkipListMap<>(memorySegmentComparator);

    @Override
    public Iterator<Entry<MemorySegment>> get(MemorySegment from, MemorySegment to) {
        if (from != null && to != null) {
            return inner.subMap(from, to).values().iterator();
        }
        if (to != null) {
            return inner.headMap(to).values().iterator();
        }
        if (from != null) {
            return inner.tailMap(from).values().iterator();
        }
        return inner.values().iterator();
    }

    @Override
    public Entry<MemorySegment> get(MemorySegment key) {
        Iterator<Entry<MemorySegment>> iterator = get(key, null);
        if (!iterator.hasNext()) {
            return null;
        }
        Entry<MemorySegment> next = iterator.next();
        if (memorySegmentComparator.compare(next.key(), key) == 0) {
            return next;
        }
        return null;
    }

    @Override
    public void upsert(Entry<MemorySegment> entry) {
        inner.put(entry.key(), entry);
    }

    static public class MemorySegmentComparator implements Comparator<MemorySegment> {

        @Override
        public int compare(MemorySegment entry1, MemorySegment entry2) {
            var firstMismatch = entry1.mismatch(entry2);
            if (firstMismatch == -1) {
                return 0;
            }
            if (firstMismatch == entry1.byteSize()) {
                return -1;
            }
            if (firstMismatch == entry2.byteSize()) {
                return 1;
            }
            return Byte.compareUnsigned(entry1.getAtIndex(ValueLayout.JAVA_BYTE, firstMismatch), entry2.getAtIndex(ValueLayout.JAVA_BYTE, firstMismatch));
        }

    }

}

