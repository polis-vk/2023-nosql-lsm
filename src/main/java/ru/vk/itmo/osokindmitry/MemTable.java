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
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class MemTable {
    private final AtomicReference<ConcurrentNavigableMap<MemorySegment, Entry<MemorySegment>>> storage;
    private final AtomicReference<ConcurrentNavigableMap<MemorySegment, Entry<MemorySegment>>> flushingStorage;
    private final AtomicLong storageSize;
    private final AtomicBoolean isFlushing;
    private final ReentrantReadWriteLock rwLock;
    private final long thresholdBytes;


    public MemTable(ConcurrentNavigableMap<MemorySegment, Entry<MemorySegment>> storage, long thresholdBytes, ReentrantReadWriteLock rwLock) {
        this.storage = new AtomicReference<>(storage);
        flushingStorage = new AtomicReference<>();
        storageSize = new AtomicLong(0);
        isFlushing = new AtomicBoolean(false);
        this.rwLock = rwLock;
        this.thresholdBytes = thresholdBytes;
    }

    public ConcurrentNavigableMap<MemorySegment, Entry<MemorySegment>> getTable() {
        rwLock.readLock().lock();
        try {
            return storage.get();
        } finally {
            rwLock.readLock().unlock();
        }
    }

    public Entry<MemorySegment> get(MemorySegment key) {
        rwLock.readLock().lock();
        try {
            Entry<MemorySegment> entry = get(key, storage);
            if (entry == null) {
                return get(key, flushingStorage);
            }
            return entry;
        } finally {
            rwLock.readLock().unlock();
        }
    }

    public List<Iterator<Entry<MemorySegment>>> get(MemorySegment from, MemorySegment to) {

        List<Iterator<Entry<MemorySegment>>> iterators = new ArrayList<>(2);

        rwLock.readLock().lock();
        try {
            iterators.add(getInMemory(from, to, storage));
            // если идет флаш, данные еще не на диске, надо вернуть данные со старого мемтейбл
            if (isFlushing.get() && flushingStorage.get() != null) {
                iterators.add(getInMemory(from, to, flushingStorage));
            }
        } finally {
            rwLock.readLock().unlock();
        }

        return iterators;
    }

    public void set(ConcurrentNavigableMap<MemorySegment, Entry<MemorySegment>> newStorage) {
        rwLock.writeLock().lock();
        try {
            this.flushingStorage.set(storage.get());
            this.storage.set(newStorage);
            storageSize.set(0);
        } finally {
            rwLock.writeLock().unlock();
        }
    }

    public void setIsFlushing(boolean isFlushing) {
        this.isFlushing.set(isFlushing);
    }

    private Entry<MemorySegment> get(
            MemorySegment key,
            AtomicReference<ConcurrentNavigableMap<MemorySegment, Entry<MemorySegment>>> storage
    ) {
        Entry<MemorySegment> entry;
        rwLock.readLock().lock();
        try {
            entry = storage.get().get(key);
        } finally {
            rwLock.readLock().unlock();
        }

        if (entry != null) {
            if (entry.value() == null) {
                return null;
            }
            return entry;
        }
        return null;
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

        rwLock.writeLock().lock();
        try {
            if (storageSize.get() + entrySize > thresholdBytes && isFlushing.get()) {
                throw new IllegalStateException();
            }
            storage.get().put(key, entry);
            storageSize.addAndGet(entrySize);
        } finally {
            rwLock.writeLock().unlock();
        }
    }

    public long size() {
        return storageSize.get();
    }

    private long entrySize(Entry<MemorySegment> entry) {
        long entrySize = entry.key().byteSize();
        if (entry.value() != null) {
            entrySize += entry.value().byteSize();
        }
        return entrySize;
    }
}