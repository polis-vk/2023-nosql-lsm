package ru.vk.itmo.trofimovmaxim;

import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.Comparator;
import java.util.Iterator;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class InMemoryDao implements Dao<MemorySegment, Entry<MemorySegment>> {

    final ConcurrentNavigableMap<MemorySegment, Entry<MemorySegment>> data;

    static final Comparator<MemorySegment> COMPARE_SEGMENT = (o1, o2) -> {
        if (o1 == null || o2 == null) {
            return o1 == null ? -1 : 1;
        }

        long o1size = o1.byteSize();
        long o2size = o2.byteSize();

        for (long i = 0; i < Math.min(o1size, o2size); ++i) {
            byte o1byte = o1.get(ValueLayout.OfByte.JAVA_BYTE, i);
            byte o2byte = o2.get(ValueLayout.OfByte.JAVA_BYTE, i);
            if (o1byte < o2byte) {
                return -1;
            } else if (o1byte > o2byte) {
                return 1;
            }
        }
        if (o1size < o2size) {
            return -1;
        } else if (o1size == o2size) {
            return 0;
        }
        return 1;
    };

    public InMemoryDao() {
        data = new ConcurrentSkipListMap<>(COMPARE_SEGMENT);
    }

    @Override
    public Iterator<Entry<MemorySegment>> get(MemorySegment from, MemorySegment to) {
        if (from == null || to == null) {
            if (from == null && to == null) {
                return data.values().iterator();
            } else if (from == null) {
                return data.headMap(to).values().iterator();
            } else {
                return data.tailMap(from).values().iterator();
            }
        } else {
            return data.subMap(from, to).values().iterator();
        }
    }

    @Override
    public Entry<MemorySegment> get(MemorySegment key) {
        return data.get(key);
    }

    @Override
    public void upsert(Entry<MemorySegment> entry) {
        data.put(entry.key(), entry);
    }
}
