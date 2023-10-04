package ru.vk.itmo.naumovivan;

import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.Iterator;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class InMemoryDao implements Dao<MemorySegment, Entry<MemorySegment>> {
    private final ConcurrentNavigableMap<MemorySegment, Entry<MemorySegment>> map =
            new ConcurrentSkipListMap<>(InMemoryDao::compareMemorySegments);

    private static int compareMemorySegments(final MemorySegment ms1, final MemorySegment ms2) {
        final long mismatch = ms1.mismatch(ms2);
        if (mismatch == -1) {
            return 0;
        }
        if (mismatch == ms1.byteSize()) {
            return -1;
        }
        if (mismatch == ms2.byteSize()) {
            return 1;
        }
        final byte b1 = ms1.get(ValueLayout.JAVA_BYTE, mismatch);
        final byte b2 = ms2.get(ValueLayout.JAVA_BYTE, mismatch);
        return Byte.compare(b1, b2);
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
