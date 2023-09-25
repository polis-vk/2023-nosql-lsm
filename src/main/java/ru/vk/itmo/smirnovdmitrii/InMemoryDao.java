package ru.vk.itmo.smirnovdmitrii;

import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Map;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class InMemoryDao implements Dao<MemorySegment, Entry<MemorySegment>> {
    private NavigableMap<MemorySegment, Entry<MemorySegment>> memorySegmentMap =
            new ConcurrentSkipListMap<>(new MemorySegmentComparator());

    private static final class MemorySegmentComparator implements Comparator<MemorySegment> {
        @Override
        public int compare(final MemorySegment o1, final MemorySegment o2) {
            long offset = o1.mismatch(o2);
            if (offset == -1) {
                return 0;
            } else if (o1.byteSize() == offset) {
                return -1;
            } else if (o2.byteSize() == offset) {
                return 1;
            }
            return Byte.compare(o1.get(ValueLayout.JAVA_BYTE, offset), o2.get(ValueLayout.JAVA_BYTE, offset));
        }
    }

    @Override
    public Iterator<Entry<MemorySegment>> get(final MemorySegment from, final MemorySegment to) {
        final Map<MemorySegment, Entry<MemorySegment>> map;
        if (from == null && to == null) {
            map = memorySegmentMap;
        } else if (from == null) {
            map = memorySegmentMap.headMap(to);
        } else if (to == null) {
            map = memorySegmentMap.tailMap(from);
        } else {
            map = memorySegmentMap.subMap(from, to);
        }
        return map.values().iterator();
    }

    @Override
    public Entry<MemorySegment> get(final MemorySegment key) {
        checkClosed();
        return key == null ? null : memorySegmentMap.get(key);
    }

    @Override
    public void upsert(final Entry<MemorySegment> entry) {
        checkClosed();
        memorySegmentMap.put(entry.key(), entry);
    }

    private void checkClosed() {
        if (memorySegmentMap == null) {
            throw new IllegalStateException("dao is closed");
        }
    }

    @Override
    public synchronized void close() {
        memorySegmentMap = null;
    }
}
