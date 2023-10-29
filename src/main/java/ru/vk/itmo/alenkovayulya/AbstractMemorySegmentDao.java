package ru.vk.itmo.alenkovayulya;

import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.Iterator;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.function.Supplier;

public abstract class AbstractMemorySegmentDao implements Dao<MemorySegment, Entry<MemorySegment>> {

    protected static final SortedMap<MemorySegment, Entry<MemorySegment>> entries =
            new ConcurrentSkipListMap<>(AbstractMemorySegmentDao::compareSegments);

    @Override
    public abstract Entry<MemorySegment> get(MemorySegment key);

    @Override
    public Iterator<Entry<MemorySegment>> get(MemorySegment from, MemorySegment to) {
        return getIterator(() -> {
            if (from == null && to == null) {
                return entries;
            } else if (from == null) {
                return entries.headMap(to);
            } else if (to == null) {
                return entries.tailMap(from);
            }
            return entries.subMap(from, to);
        });

    }

    @Override
    public void upsert(Entry<MemorySegment> entry) {
        entries.put(entry.key(), entry);

    }

    public static int compareSegments(MemorySegment memorySegment1, MemorySegment memorySegment2) {
        long mismatchOffset = memorySegment1.mismatch(memorySegment2);
        if (mismatchOffset == -1) {
            return 0;
        } else if (mismatchOffset == memorySegment1.byteSize()) {
            return -1;
        } else if (mismatchOffset == memorySegment2.byteSize()) {
            return 1;
        }
        return memorySegment1.get(ValueLayout.JAVA_BYTE, mismatchOffset)
                - memorySegment2.get(ValueLayout.JAVA_BYTE, mismatchOffset);
    }

    private Iterator<Entry<MemorySegment>> getIterator(Supplier<SortedMap<MemorySegment, Entry<MemorySegment>>> map) {
        return map.get().values().iterator();
    }

}
