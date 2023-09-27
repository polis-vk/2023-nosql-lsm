package ru.vk.itmo.cheshevandrey;

import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.Iterator;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class InMemoryDao implements Dao<MemorySegment, Entry<MemorySegment>> {

    private final ConcurrentNavigableMap<MemorySegment, Entry<MemorySegment>> map = new ConcurrentSkipListMap<>(
            (seg1, seg2) -> {
                long segSize1 = seg1.byteSize();
                long segSize2 = seg2.byteSize();

                if (segSize1 == 0 && segSize2 == 0) {
                    return 0;
                } else if (segSize1 > segSize2 || segSize1 == 0) {
                    return -1;
                } else if (segSize1 < segSize2) {
                    return 1;
                }

                int offset = 0;
                while (offset < segSize1) {
                    byte byte1 = seg1.get(ValueLayout.JAVA_BYTE, offset);
                    byte byte2 = seg2.get(ValueLayout.JAVA_BYTE, offset);

                    int compareResult = Byte.compare(byte1, byte2);
                    if (compareResult != 0) {
                        return compareResult;
                    }

                    offset++;
                }

                return 0;
            }
    );

    @Override
    public Entry<MemorySegment> get(MemorySegment key) {
        return map.get(key);
    }

    @Override
    public Iterator<Entry<MemorySegment>> get(MemorySegment from, MemorySegment to) {

        if (from == null && to == null) {
            return map.values().iterator();
        } else if (from == MemorySegment.NULL) {
            if (to == MemorySegment.NULL) {
                return map.headMap(from, true).values().iterator();
            } else {
                return map.subMap(map.firstKey(), to).values().iterator();
            }
        } else {
            return map.subMap(from, to).values().iterator();
        }
    }

    @Override
    public void upsert(Entry<MemorySegment> entry) {
        map.put(entry.key(), entry);
    }
}
