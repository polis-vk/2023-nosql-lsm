package ru.vk.itmo.proninvalentin;

import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.Iterator;
import java.util.concurrent.ConcurrentSkipListMap;

public class InMemoryDao implements Dao<MemorySegment, Entry<MemorySegment>> {
    private final ConcurrentSkipListMap<MemorySegment, Entry<MemorySegment>> memorySegments;

    public InMemoryDao() {
        memorySegments = new ConcurrentSkipListMap<>(InMemoryDao::compare);
    }

    private static int compare(MemorySegment o1, MemorySegment o2) {
        var mismatchOffset = o1.mismatch(o2);
        if (mismatchOffset == -1) {
            return 0;
        } else if (mismatchOffset == o1.byteSize()) {
            return -1;
        } else if (mismatchOffset == o2.byteSize()) {
            return 1;
        }

        return Byte.compare(
                o1.get(ValueLayout.JAVA_BYTE, mismatchOffset),
                o2.get(ValueLayout.JAVA_BYTE, mismatchOffset));
    }

    @Override
    public Iterator<Entry<MemorySegment>> all() {
        return memorySegments.values().iterator();
    }

    @Override
    public Iterator<Entry<MemorySegment>> allFrom(MemorySegment from) {
        return memorySegments.tailMap(from).values().iterator();
    }

    @Override
    public Iterator<Entry<MemorySegment>> allTo(MemorySegment to) {
        return memorySegments.headMap(to).values().iterator();
    }

    @Override
    public Iterator<Entry<MemorySegment>> get(MemorySegment from, MemorySegment to) {
        if (from == null && to == null) {
            return all();
        } else if (to == null) {
            return allFrom(from);
        } else if (from == null) {
            return allTo(to);
        } else {
            return memorySegments.tailMap(from).headMap(to).values().iterator();
        }
    }

    @Override
    public Entry<MemorySegment> get(MemorySegment key) {
        return memorySegments.get(key);
    }

    @Override
    public void upsert(Entry<MemorySegment> entry) {
        memorySegments.put(entry.key(), entry);
    }

    @Override
    public void flush() throws IOException {
        Dao.super.flush();
    }

    @Override
    public void close() throws IOException {
        Dao.super.close();
    }
}
