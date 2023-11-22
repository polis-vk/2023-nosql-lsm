package ru.vk.itmo.danilinandrew;

import ru.vk.itmo.Entry;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;

public class MemTable {
    private final Comparator<MemorySegment> comparator = StorageDao::compare;
    private NavigableMap<MemorySegment, Entry<MemorySegment>> storage = new ConcurrentSkipListMap<>(comparator);
    private NavigableMap<MemorySegment, Entry<MemorySegment>> flushingStorage;
    private final AtomicLong sizeInBytes = new AtomicLong();
    private final long flushThresholdBytes;

    public MemTable(long flushThresholdBytes) {
        this.flushThresholdBytes = flushThresholdBytes;
    }

    public MemTable(
        NavigableMap<MemorySegment, Entry<MemorySegment>> storage,
        NavigableMap<MemorySegment, Entry<MemorySegment>> flushingStorage,
        long flushThresholdBytes
    ) {
        this.storage = storage;
        this.flushThresholdBytes = flushThresholdBytes;
        this.flushingStorage = flushingStorage;
    }


    public NavigableMap<MemorySegment, Entry<MemorySegment>> getStorage () {
        return storage;
    }

    public NavigableMap<MemorySegment, Entry<MemorySegment>> getFlushingStorage () {
        return flushingStorage;
    }

    public Iterator<Entry<MemorySegment>> get(MemorySegment from, MemorySegment to) {
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

    public Entry<MemorySegment> get(MemorySegment key) {
        return storage.get(key);
    }

    public boolean upsert(Entry<MemorySegment> entry, AtomicBoolean isFlushing) {
        long sizeInBytesToAdd = entry.key().byteSize() +
                (entry.value() == null ? 0 : entry.value().byteSize());

        long newSizeInBytes = sizeInBytes.addAndGet(sizeInBytesToAdd);

        if (newSizeInBytes > flushThresholdBytes) {
            isFlushing.getAndSet(true);
            return true;
        }

        storage.put(entry.key(), entry);

        return false;
    }

    public boolean isEmpty() {
        return storage.isEmpty();
    }

    public Collection<Entry<MemorySegment>> values() {
        return storage.values();
    }
}
