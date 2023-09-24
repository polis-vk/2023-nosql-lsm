package ru.vk.itmo.alenkovayulya;

import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.Collections;
import java.util.Iterator;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class InMemoryDao implements Dao<MemorySegment, Entry<MemorySegment>> {

    private final NavigableMap<MemorySegment, Entry<MemorySegment>> entries;

    public InMemoryDao() {
        entries = new ConcurrentSkipListMap<>(this::compareSegments);
    }

    @Override
    public Iterator<Entry<MemorySegment>> get(MemorySegment from, MemorySegment to) {

        if (entries.isEmpty()) {
            return Collections.emptyIterator();
        }

        return entries.subMap(
                from == null ? entries.firstKey() : from, true,
                to == null ? entries.lastKey() : to, to == null).values().iterator();
    }

    @Override
    public void upsert(Entry<MemorySegment> entry) {
        entries.put(entry.key(), entry);

    }

    @Override
    public Entry<MemorySegment> get(MemorySegment key) {
        return entries.get(key);
    }

    private int compareSegments(MemorySegment memorySegment1, MemorySegment memorySegment2) {
        long mismatchOffset = memorySegment1.mismatch(memorySegment2);
        if (mismatchOffset == -1) {
            return 0;
        } else if (mismatchOffset == memorySegment1.byteSize()) {
            return -1;
        } else if (mismatchOffset == memorySegment2.byteSize()) {
            return 1;
        }
        return Byte.compare(
                memorySegment1.get(ValueLayout.JAVA_BYTE, mismatchOffset),
                memorySegment2.get(ValueLayout.JAVA_BYTE, mismatchOffset));
    }

}
