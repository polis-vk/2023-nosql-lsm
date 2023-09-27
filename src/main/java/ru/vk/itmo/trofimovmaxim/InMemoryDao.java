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

    private final ConcurrentNavigableMap<MemorySegment, Entry<MemorySegment>> data;

    private static final Comparator<MemorySegment> COMPARE_SEGMENT = (o1, o2) -> {
        if (o1 == null || o2 == null) {
            return o1 == null ? -1 : 1;
        }

        long mism = o1.mismatch(o2);
        if (mism == -1) {
            return (int) (o1.byteSize() - o2.byteSize());
        }
        if (mism == o1.byteSize() || mism == o2.byteSize()) {
            return mism == o1.byteSize() ? -1 : 1;
        }
        return Byte.compare(
                o1.get(ValueLayout.OfByte.JAVA_BYTE, mism),
                o2.get(ValueLayout.OfByte.JAVA_BYTE, mism)
        );
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
