package ru.vk.itmo.osokindmitry;

import ru.vk.itmo.Entry;

import java.lang.foreign.MemorySegment;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

public class MemTable {
    private final AtomicReference<ConcurrentNavigableMap<MemorySegment, Entry<MemorySegment>>> storage;
    private final AtomicReference<ConcurrentNavigableMap<MemorySegment, Entry<MemorySegment>>> flushingStorage;
    private final AtomicLong storageSize;
    private final AtomicBoolean isFlushing;
    private final long thresholdBytes;

    public MemTable(ConcurrentNavigableMap<MemorySegment, Entry<MemorySegment>> storage, long thresholdBytes) {
        this.storage = new AtomicReference<>(storage);
        flushingStorage = new AtomicReference<>();
        storageSize = new AtomicLong(0);
        isFlushing = new AtomicBoolean(false);
        this.thresholdBytes = thresholdBytes;
    }

    public ConcurrentNavigableMap<MemorySegment, Entry<MemorySegment>> getTable() {
        return storage.get();
    }

    public ConcurrentNavigableMap<MemorySegment, Entry<MemorySegment>> getFlushingTable() {
        return flushingStorage.get();
    }

    public Entry<MemorySegment> get(MemorySegment key) {
        Entry<MemorySegment> entry = get(key, storage);
        if (entry == null && isFlushing.get()) {
            return get(key, flushingStorage);
        }
        return entry;
    }

    public List<Iterator<Entry<MemorySegment>>> get(MemorySegment from, MemorySegment to) {

        List<Iterator<Entry<MemorySegment>>> iterators = new ArrayList<>(2);

        if (isFlushing.get() && flushingStorage.get() != null) {
            iterators.add(getInMemory(from, to, flushingStorage));
        }
        iterators.add(getInMemory(from, to, storage));

        return iterators;
    }

    public void set(ConcurrentNavigableMap<MemorySegment, Entry<MemorySegment>> newStorage) {
        this.flushingStorage.set(storage.get());
        this.storage.set(newStorage);
        storageSize.set(0);
    }

    public void setIsFlushing(boolean isFlushing) {
        this.isFlushing.set(isFlushing);
    }

    public boolean isNotFlushing() {
        return !this.isFlushing.get();
    }

    public long size() {
        return storageSize.get();
    }

    public long getThresholdBytes() {
        return thresholdBytes;
    }

    private Entry<MemorySegment> get(
            MemorySegment key,
            AtomicReference<ConcurrentNavigableMap<MemorySegment, Entry<MemorySegment>>> storage
    ) {
        return storage.get().get(key);
    }

    private Iterator<Entry<MemorySegment>> getInMemory(
            MemorySegment from,
            MemorySegment to,
            AtomicReference<ConcurrentNavigableMap<MemorySegment, Entry<MemorySegment>>> storage
    ) {
        if (from == null && to == null) {
            return storage.get().values().iterator();
        }
        if (from == null) {
            return storage.get().headMap(to).values().iterator();
        }
        if (to == null) {
            return storage.get().tailMap(from).values().iterator();
        }
        return storage.get().subMap(from, to).values().iterator();
    }

    public void put(MemorySegment key, Entry<MemorySegment> entry) {
        long entrySize = entrySize(entry);

        storageSize.addAndGet(entrySize);

        if (storageSize.get() > thresholdBytes && isFlushing.get()) {
            throw new IllegalStateException();
        }
        storage.get().put(key, entry);
    }

    private long entrySize(Entry<MemorySegment> entry) {
        long entrySize = entry.key().byteSize();
        if (entry.value() != null) {
            entrySize += entry.value().byteSize();
        }
        return entrySize;
    }

}
