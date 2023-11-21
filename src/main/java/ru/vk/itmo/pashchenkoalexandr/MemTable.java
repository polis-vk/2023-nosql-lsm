package ru.vk.itmo.pashchenkoalexandr;

import java.lang.foreign.MemorySegment;
import java.util.Comparator;
import java.util.Iterator;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;

import ru.vk.itmo.Entry;

public class MemTable {
    private final long flushThresholdBytes;
    private final Comparator<MemorySegment> comparator = PaschenkoDao::compare;
    private final NavigableMap<MemorySegment, Entry<MemorySegment>> storage = new ConcurrentSkipListMap<>(comparator);
    private final AtomicLong size = new AtomicLong();

    public MemTable(long flushThresholdBytes) {
        this.flushThresholdBytes = flushThresholdBytes;
    }

    public boolean upsert(Entry<MemorySegment> entry) {
        long toAdd = entrySize(entry);
        long newSize = size.addAndGet(toAdd);
        if (newSize - toAdd > flushThresholdBytes) {
            return false;
        }
        storage.put(entry.key(), entry);
        return true;
    }

    public Iterator<Entry<MemorySegment>> getInMemory(MemorySegment from, MemorySegment to) {
        if (from == null && to == null) {
            return storage.values().iterator();
        }
        if (from == null) {
            return storage.headMap(to).values().iterator();
        }
        if (to == null) {
            return storage.tailMap(from).values().iterator();
        }
        return storage.subMap(from, to).values().iterator();
    }

    public NavigableMap<MemorySegment, Entry<MemorySegment>> getStorage() {
        return storage;
    }

    public static long entrySize(Entry<MemorySegment> entry) {
        return entry.key().byteSize()
                + (entry.value() == null ? 0 : entry.value().byteSize())
                + 2 * Long.BYTES;
    }

    public long getSize() {
        return size.get();
    }
}
