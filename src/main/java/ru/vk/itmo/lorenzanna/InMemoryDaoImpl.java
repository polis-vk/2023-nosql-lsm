package ru.vk.itmo.lorenzanna;

import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.Comparator;
import java.util.Iterator;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class InMemoryDaoImpl implements Dao<MemorySegment, Entry<MemorySegment>> {
    private final MemorySegmentComparator memorySegmentComparator = new MemorySegmentComparator();
    private final ConcurrentNavigableMap<MemorySegment, Entry<MemorySegment>> memorySegmentEntriesMap =
            new ConcurrentSkipListMap<>(memorySegmentComparator);

    @Override
    public Iterator<Entry<MemorySegment>> get(MemorySegment from, MemorySegment to) {
        if (from != null && to != null) {
            return memorySegmentEntriesMap.subMap(from, to).values().iterator();
        } else if (from != null) {
            return memorySegmentEntriesMap.tailMap(from).values().iterator();
        } else if (to != null) {
            return memorySegmentEntriesMap.headMap(to).values().iterator();
        }
        return memorySegmentEntriesMap.values().iterator();
    }

    @Override
    public Entry<MemorySegment> get(MemorySegment key) {
        Iterator<Entry<MemorySegment>> iterator = get(key, null);

        if (!iterator.hasNext()) {
            return null;
        }
        Entry<MemorySegment> next = iterator.next();

        if (memorySegmentComparator.compare(key, next.key()) == 0) {
            return next;
        }

        return null;
    }

    @Override
    public void upsert(Entry<MemorySegment> entry) {
        memorySegmentEntriesMap.put(entry.key(), entry);
    }

    private static class MemorySegmentComparator implements Comparator<MemorySegment> {
        @Override
        public int compare(MemorySegment o1, MemorySegment o2) {
            var relativeOffset = o1.mismatch(o2);

            if (relativeOffset == -1) {
                return 0;
            } else if (relativeOffset == o1.byteSize()) {
                return -1;
            } else if (relativeOffset == o2.byteSize()) {
                return 1;
            }

            return Byte.compare(o1.get(ValueLayout.JAVA_BYTE, relativeOffset), o2.get(ValueLayout.JAVA_BYTE, relativeOffset));
        }
    }
}
