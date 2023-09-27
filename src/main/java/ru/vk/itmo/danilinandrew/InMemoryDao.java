package ru.vk.itmo.danilinandrew;

import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.Iterator;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class InMemoryDao implements Dao<MemorySegment, Entry<MemorySegment>> {
    private final ConcurrentNavigableMap<MemorySegment, Entry<MemorySegment>> data = new ConcurrentSkipListMap<>(new MemorySegmentComparator());
    @Override
    public Iterator<Entry<MemorySegment>> get(MemorySegment from, MemorySegment to) {
        if (from == null) {
            from = MemorySegment.NULL;
        }

        if (to == null) {
            return data.tailMap(from).values().iterator();
        }

        return data.subMap(from, to).values().iterator();
    }

    @Override
    public void upsert(Entry<MemorySegment> entry) {
        if (entry == null) {
            return;
        }

        data.put(entry.key(), entry);
    }
    static class MemorySegmentComparator implements Comparator<MemorySegment> {
        @Override
        public int compare(MemorySegment o1, MemorySegment o2) {
            int offset = 0;

            while (offset < o1.byteSize() && offset < o2.byteSize()) {
                byte byte1 = o1.get(ValueLayout.JAVA_BYTE, offset);
                byte byte2 = o2.get(ValueLayout.JAVA_BYTE, offset);

                int compareRes = Byte.compare(byte1, byte2);

                if (compareRes == 0) {
                    offset++;
                } else {
                    return compareRes;
                }
            }

            return Long.compare(o1.byteSize(), o2.byteSize());
        }
    }
}
