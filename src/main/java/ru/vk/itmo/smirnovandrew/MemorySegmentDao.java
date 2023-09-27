package ru.vk.itmo.smirnovandrew;

import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.Comparator;
import java.util.Iterator;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class MemorySegmentDao implements Dao<MemorySegment, Entry<MemorySegment>> {

    private final NavigableMap<MemorySegment, Entry<MemorySegment>> segments =
            new ConcurrentSkipListMap<>(segmentComparator);

    private static final Comparator<MemorySegment> segmentComparator = (o1, o2) -> {
        if (o1.byteSize() != o2.byteSize()) {
            return Long.compare(o1.byteSize(), o2.byteSize());
        }

        long mismatch = o1.mismatch(o2);
        if (mismatch < 0) {
            return 0;
        }
        return Byte.compare(o1.getAtIndex(ValueLayout.JAVA_BYTE, mismatch),
                o2.getAtIndex(ValueLayout.JAVA_BYTE, mismatch));
    };

    @Override
    public Iterator<Entry<MemorySegment>> get(MemorySegment from, MemorySegment to) {

        if (from == null && to == null) {
            return segments.values().iterator();
        }
        if (from == null) {
            return segments.headMap(to).values().iterator();
        }
        if (to == null) {
            return segments.tailMap(from).values().iterator();
        }

        return segments.subMap(from, true, to, false).values().iterator();
    }

    @Override
    public Entry<MemorySegment> get(MemorySegment key) {
        return segments.get(key);
    }

    @Override
    public void upsert(Entry<MemorySegment> entry) {
        segments.put(entry.key(), entry);
    }
}
