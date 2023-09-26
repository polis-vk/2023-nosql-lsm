package ru.vk.itmo.test.ryabovvadim;

import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.Comparator;
import java.util.Iterator;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class InMemoryDao implements Dao<MemorySegment, Entry<MemorySegment>> {
    private static final Comparator<MemorySegment> MEMORY_SEGMENT_COMPARATOR =
            (firstSegment, secondSegment) -> {
                long firstSegmentSize = firstSegment.byteSize();
                long secondSegmentSize = secondSegment.byteSize();
                byte firstSegmentByte;
                byte secondSegmentByte;

                for (long i = 0; i < Math.min(firstSegmentSize, secondSegmentSize); ++i) {
                    firstSegmentByte = firstSegment.getAtIndex(ValueLayout.JAVA_BYTE, i);
                    secondSegmentByte = secondSegment.getAtIndex(ValueLayout.JAVA_BYTE, i);

                    if (firstSegmentByte != secondSegmentByte) {
                        return Byte.compare(firstSegmentByte, secondSegmentByte);
                    }
                }

                return Long.compare(firstSegmentSize, secondSegmentSize);
            };
    private final ConcurrentNavigableMap<MemorySegment, Entry<MemorySegment>> storage =
            new ConcurrentSkipListMap<>(MEMORY_SEGMENT_COMPARATOR);

    @Override
    public Entry<MemorySegment> get(MemorySegment key) {
        return storage.get(key);
    }

    @Override
    public Iterator<Entry<MemorySegment>> allFrom(MemorySegment from) {
        if (from == null) {
            return all();
        }
        return storage.tailMap(from).values().iterator();
    }

    @Override
    public Iterator<Entry<MemorySegment>> allTo(MemorySegment to) {
        if (to == null) {
            return all();
        }
        return storage.headMap(to).values().iterator();
    }

    @Override
    public Iterator<Entry<MemorySegment>> all() {
        return storage.values().iterator();
    }

    @Override
    public Iterator<Entry<MemorySegment>> get(MemorySegment from, MemorySegment to) {
        if (from == null) {
            return allTo(to);
        }
        if (to == null) {
            return allFrom(from);
        }
        return storage.subMap(from, to).values().iterator();
    }

    @Override
    public void upsert(Entry<MemorySegment> entry) {
        storage.put(entry.key(), entry);
    }
}
