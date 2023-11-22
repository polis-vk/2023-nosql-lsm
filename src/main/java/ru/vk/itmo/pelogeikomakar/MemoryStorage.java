package ru.vk.itmo.pelogeikomakar;

import ru.vk.itmo.Entry;

import java.lang.foreign.MemorySegment;
import java.util.Comparator;
import java.util.Iterator;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicReference;

public class MemoryStorage {
    private static final String DEF_NAME = "Memory";
    private NavigableMap<MemorySegment, Entry<MemorySegment>> storage;
    private final Comparator<MemorySegment> comparator;
    private long currentByteSize;
    private volatile boolean flushStatus = false;
    private AtomicReference<String> atomicTableName;

    public MemoryStorage(Comparator<MemorySegment> comparator) {
        this.comparator = comparator;
        storage = new ConcurrentSkipListMap<>(this.comparator);
        atomicTableName = new AtomicReference<>(DEF_NAME);
        currentByteSize = 0;
    }

    public SegmentIterInterface getInMemory(MemorySegment from, MemorySegment to) {
        Iterator<Entry<MemorySegment>> iter;
        if (from == null && to == null) {
            iter = storage.values().iterator();
        } else if  (from == null) {
            iter = storage.headMap(to).values().iterator();
        } else if (to == null) {
            iter = storage.tailMap(from).values().iterator();
        } else {
            iter = storage.subMap(from, to).values().iterator();
        }

        return new SegmentMemIterator(iter, DEF_NAME, atomicTableName);

    }

    public void put(MemorySegment key, Entry<MemorySegment> entry) {
        long additionSize = key.byteSize() + entry.key().byteSize() + (entry.value() == null ? 0: entry.value().byteSize());
        Entry<MemorySegment> currEntry = storage.get(key);
        long subtractionSize = 0;
        if (currEntry != null) {
            subtractionSize = key.byteSize() + currEntry.key().byteSize() + (currEntry.value() == null ? 0: currEntry.value().byteSize());
        }
        storage.put(key, entry);
        currentByteSize += additionSize - subtractionSize;
    }

    public Entry<MemorySegment> get(MemorySegment key) {
        return storage.get(key);
    }

    public boolean isEmpty() {
        return storage.isEmpty();
    }

    public Iterable<Entry<MemorySegment>> values() {
        return storage.values();
    }
    public int quantity() {
        return storage.size();
    }
    public long byteSize() {
        return currentByteSize;
    }

    public void makeNewStorage() {
        storage = new ConcurrentSkipListMap<>(this.comparator);
        atomicTableName = new AtomicReference<>(DEF_NAME);
    }

    public AtomicReference<String> getAtomicTableName() {
        return atomicTableName;
    }

    public boolean getFlushStatus() {
        return flushStatus;
    }

    public void setFlushStatus(boolean flushStatus) {
        this.flushStatus = flushStatus;
    }
}
