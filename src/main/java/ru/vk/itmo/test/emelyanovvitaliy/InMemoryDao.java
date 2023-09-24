package ru.vk.itmo.test.emelyanovvitaliy;

import ru.vk.itmo.BaseEntry;
import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;

import java.lang.foreign.MemorySegment;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;

public class InMemoryDao implements Dao<MemorySegment, Entry<MemorySegment>> {
    private final Comparator<MemorySegment> comparator = (o1, o2) -> {
        if (o1 == o2) {
            return 0;
        }
        if (o1.byteSize() != o2.byteSize()) {
            return Long.compare(o1.byteSize(), o2.byteSize());
        }
        return o1.asByteBuffer().compareTo(o2.asByteBuffer());
    };
    private final ConcurrentSkipListMap<MemorySegment, MemorySegment> mappings = new ConcurrentSkipListMap<>(
            comparator
    );

    @Override
    public Entry<MemorySegment> get(MemorySegment key) {
        return new BaseEntry<>(key, mappings.get(key));
    }

    @Override
    public Iterator<Entry<MemorySegment>> get(MemorySegment from, MemorySegment to) {
        Iterator<Map.Entry<MemorySegment, MemorySegment>> iterator = privateGet(from, to);
        return new Iterator<>() {
            @Override
            public boolean hasNext() {
                return iterator.hasNext();
            }

            @Override
            public Entry<MemorySegment> next() {
                Map.Entry<MemorySegment, MemorySegment> result = iterator.next();
                return new BaseEntry<>(result.getKey(), result.getValue());
            }
        };
    }

    @Override
    public void upsert(Entry<MemorySegment> entry) {
        mappings.put(entry.key(), entry.value());
    }

    private Iterator<Map.Entry<MemorySegment, MemorySegment>> privateGet(MemorySegment from, MemorySegment to) {
        if (from == null && to == null) {
            return mappings.entrySet().iterator();
        } else if (from == null) {
            return mappings.headMap(to).entrySet().iterator();
        } else if (to == null) {
            return mappings.headMap(from).entrySet().iterator();
        }
        return mappings.subMap(from, to)
                .entrySet().iterator();
    }

}
