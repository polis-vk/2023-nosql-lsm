package ru.vk.itmo.trofimovmaxim;

import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.Arrays;
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
        return Arrays.compare(
                o1.toArray(ValueLayout.OfByte.JAVA_BYTE),
                o2.toArray(ValueLayout.OfByte.JAVA_BYTE)
        );
    };

    public InMemoryDao() {
        data = new ConcurrentSkipListMap<>(COMPARE_SEGMENT);
    }

    @Override
    public Iterator<Entry<MemorySegment>> get(MemorySegment from, MemorySegment to) {
        return new DaoIter(from, to);
    }

    @Override
    public Entry<MemorySegment> get(MemorySegment key) {
        return data.get(key);
    }

    @Override
    public void upsert(Entry<MemorySegment> entry) {
        data.put(entry.key(), entry);
    }

    private class DaoIter implements Iterator<Entry<MemorySegment>> {
        Iterator<Entry<MemorySegment>> it;

        DaoIter(MemorySegment from, MemorySegment to) {
            if (from == null || to == null) {
                if (from == null && to == null) {
                    it = data.values().iterator();
                } else if (from == null) {
                    it = data.headMap(to).values().iterator();
                } else {
                    it = data.tailMap(from).values().iterator();
                }
            } else {
                it = data.subMap(from, to).values().iterator();
            }
        }

        @Override
        public boolean hasNext() {
            return it.hasNext();
        }

        @Override
        public Entry<MemorySegment> next() {
            return it.next();
        }
    }
}
