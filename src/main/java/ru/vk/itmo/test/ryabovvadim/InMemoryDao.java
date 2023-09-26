package ru.vk.itmo.test.ryabovvadim;

import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class InMemoryDao implements Dao<MemorySegment, Entry<MemorySegment>> {
    private static final Comparator<MemorySegment> MEMORY_SEGMENT_COMPARATOR =
            Comparator.comparingLong(MemorySegment::byteSize)
                    .thenComparing(
                            memorySegment -> memorySegment.toArray(ValueLayout.JAVA_BYTE),
                            Arrays::compare
                    );
    private final ConcurrentNavigableMap<MemorySegment, Entry<MemorySegment>> storage =
            new ConcurrentSkipListMap<>(MEMORY_SEGMENT_COMPARATOR);

    @Override
    public Entry<MemorySegment> get(MemorySegment key) {
        return storage.get(key);
    }

    @Override
    public Iterator<Entry<MemorySegment>> allFrom(MemorySegment from) {
        if (checkNullMemorySegment(from)) {
            return all();
        }
        return getValuesIterator(storage.tailMap(from));
    }

    @Override
    public Iterator<Entry<MemorySegment>> allTo(MemorySegment to) {
        if (checkNullMemorySegment(to)) {
            return all();
        }
        return getValuesIterator(storage.headMap(to));
    }

    @Override
    public Iterator<Entry<MemorySegment>> all() {
        return getValuesIterator(storage);
    }

    @Override
    public Iterator<Entry<MemorySegment>> get(MemorySegment from, MemorySegment to) {
        if (checkNullMemorySegment(from)) {
            return allTo(to);
        }
        if (checkNullMemorySegment(to)) {
            return allFrom(from);
        }
        return getValuesIterator(storage.subMap(from, to));
    }

    @Override
    public void upsert(Entry<MemorySegment> entry) {
        storage.put(entry.key(), entry);
    }

    private boolean checkNullMemorySegment(MemorySegment memorySegment) {
        return memorySegment == null || MemorySegment.NULL.equals(memorySegment);
    }

    private Iterator<Entry<MemorySegment>> getValuesIterator(
            Map<MemorySegment, Entry<MemorySegment>> map
    ) {
        return map.values().iterator();
    }
}
