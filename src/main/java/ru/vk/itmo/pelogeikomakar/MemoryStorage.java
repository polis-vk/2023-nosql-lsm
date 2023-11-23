package ru.vk.itmo.pelogeikomakar;

import ru.vk.itmo.Entry;

import java.lang.foreign.MemorySegment;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class MemoryStorage {
    private NavigableMap<MemorySegment, Entry<MemorySegment>> storage;
    private final Comparator<MemorySegment> comparator;
    private volatile long currentByteSize;
    private final ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock();
    private final Lock readLock = rwLock.readLock();
    private final Lock writeLock = rwLock.writeLock();

    public MemoryStorage(Comparator<MemorySegment> comparator) {
        this.comparator = comparator;
        storage = new ConcurrentSkipListMap<>(this.comparator);
        currentByteSize = 0;
    }

    public Iterator<Entry<MemorySegment>> getInMemory(MemorySegment from, MemorySegment to) {
        Iterator<Entry<MemorySegment>> iter;
        readLock.lock();
        try {
            if (from == null && to == null) {
                iter = storage.values().iterator();
            } else if (from == null) {
                iter = storage.headMap(to).values().iterator();
            } else if (to == null) {
                iter = storage.tailMap(from).values().iterator();
            } else {
                iter = storage.subMap(from, to).values().iterator();
            }
        } finally {
            readLock.unlock();
        }

        return iter;
    }

    public void put(MemorySegment key, Entry<MemorySegment> entry, long newSize) {
        readLock.lock();
        try {
            storage.put(key, entry);
            currentByteSize = newSize;
        } finally {
            readLock.unlock();
        }
    }

    public long newIfput(MemorySegment key, Entry<MemorySegment> entry) {
        long additionSize = entry.key().byteSize() + (entry.value() == null ? 0 : entry.value().byteSize());
        synchronized (this) {
            Entry<MemorySegment> currEntry = get(key);
            if (currEntry == null) {
                return currentByteSize + additionSize;
            } else {
                long actSize = currEntry.key().byteSize() + (currEntry.value() == null ? 0 : currEntry.value().byteSize());
                return currentByteSize + additionSize - actSize;
            }
        }
    }

    public Entry<MemorySegment> get(MemorySegment key) {
        return storage.get(key);
    }

    public boolean isEmpty() {
        return storage.isEmpty();
    }

    public long byteSize() {
        return currentByteSize;
    }

    public Collection<Entry<MemorySegment>> prepareFlash() {
        var newStorage = new ConcurrentSkipListMap<MemorySegment, Entry<MemorySegment>>(this.comparator);
        writeLock.lock();
        try {
            var values = storage.values();
            storage = newStorage;
            return values;
        } finally {
            writeLock.unlock();
        }
    }
}
