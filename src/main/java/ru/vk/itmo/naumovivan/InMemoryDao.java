package ru.vk.itmo.naumovivan;

import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.Iterator;
import java.util.concurrent.ConcurrentSkipListMap;

public class InMemoryDao implements Dao<MemorySegment, Entry<MemorySegment>> {
    private final ConcurrentSkipListMap<MemorySegment, Entry<MemorySegment>> map =
            new ConcurrentSkipListMap<>(InMemoryDao::compareMemorySegments);

    private static int compareMemorySegments(final MemorySegment ms1, final MemorySegment ms2) {
        final long n1 = ms1.byteSize();
        final long n2 = ms2.byteSize();
        for (long i = 0; i < n1 && i < n2; ++i) {
            final byte b1 = ms1.get(ValueLayout.JAVA_BYTE, i);
            final byte b2 = ms2.get(ValueLayout.JAVA_BYTE, i);
            if (b1 != b2) {
                return Byte.compare(b1, b2);
            }
        }
        return Long.compare(n1, n2);
    }

    @Override
    public Iterator<Entry<MemorySegment>> get(final MemorySegment from, final MemorySegment to) {
        if (from == null && to == null) {
            return map.values().iterator();
        } else if (from == null) {
            return map.headMap(to).values().iterator();
        } else if (to == null) {
            return map.tailMap(from).values().iterator();
        } else {
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
