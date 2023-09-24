package ru.vk.itmo.test.emelyanovvitaliy;

import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;

import java.lang.foreign.MemorySegment;
import java.util.Comparator;
import java.util.Iterator;
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
    private final ConcurrentSkipListMap<MemorySegment, Entry<MemorySegment>> mappings = new ConcurrentSkipListMap<>(
            comparator
    );

    @Override
    public Entry<MemorySegment> get(MemorySegment key) {
        return mappings.get(key);
    }

    @Override
    public Iterator<Entry<MemorySegment>> get(MemorySegment from, MemorySegment to) {
        if (from == null && to == null) {
            return mappings.values().iterator();
        } else if (from == null) {
            return mappings.headMap(to).values().iterator();
        } else if (to == null) {
            return mappings.headMap(from).values().iterator();
        }
        return mappings.subMap(from, to)
                .sequencedValues().iterator();
    }

    @Override
    public void upsert(Entry<MemorySegment> entry) {
        mappings.put(entry.key(), entry);
    }
}
