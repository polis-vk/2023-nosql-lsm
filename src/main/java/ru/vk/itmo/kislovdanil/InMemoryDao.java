package ru.vk.itmo.kislovdanil;

import ru.vk.itmo.BaseEntry;
import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class InMemoryDao implements Dao<MemorySegment, Entry<MemorySegment>> {
    ConcurrentSkipListMap<MemorySegment, MemorySegment> storage = new ConcurrentSkipListMap<>(new MemSegComparator());

    private static class MemSegComparator implements Comparator<MemorySegment> {
        @Override
        public int compare(MemorySegment o1, MemorySegment o2) {
            long mismatch = o1.mismatch(o2);
            if (mismatch == -1) {
                return 0;
            }
            if (mismatch == Math.min(o1.byteSize(), o2.byteSize())) {
                return Long.compare(o1.byteSize(), o2.byteSize());
            }
            return Byte.compare(o1.get(ValueLayout.JAVA_BYTE, mismatch), o2.get(ValueLayout.JAVA_BYTE, mismatch));
        }
    }

    private static class IteratorWrapper implements Iterator<Entry<MemorySegment>> {
        Iterator<Map.Entry<MemorySegment, MemorySegment>> innerIt;

        public IteratorWrapper(Iterator<Map.Entry<MemorySegment, MemorySegment>> it) {
            this.innerIt = it;
        }

        @Override
        public boolean hasNext() {
            return innerIt.hasNext();
        }

        @Override
        public Entry<MemorySegment> next() {
            Map.Entry<MemorySegment, MemorySegment> data = innerIt.next();
            return new BaseEntry<>(data.getKey(), data.getValue());
        }
    }

    @Override
    public Iterator<Entry<MemorySegment>> get(MemorySegment from, MemorySegment to) {
        ConcurrentNavigableMap<MemorySegment, MemorySegment> subMap = storage;
        if (from != null) subMap = subMap.tailMap(from);
        if (to != null) subMap = subMap.headMap(to);
        Iterator<Map.Entry<MemorySegment, MemorySegment>> it = subMap.entrySet().iterator();
        return new IteratorWrapper(it);
    }

    @Override
    public Entry<MemorySegment> get(MemorySegment key) {
        return new BaseEntry<>(key, storage.get(key));
    }

    @Override
    public void upsert(Entry<MemorySegment> entry) {
        storage.put(entry.key(), entry.value());
    }
}
