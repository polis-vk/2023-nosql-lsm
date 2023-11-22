package ru.vk.itmo.shemetovalexey;

import ru.vk.itmo.Config;
import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class InMemoryDao implements Dao<MemorySegment, Entry<MemorySegment>> {
    private static final Comparator<MemorySegment> comparator = InMemoryDao::compare;
    private final NavigableMap<MemorySegment, Entry<MemorySegment>> memoryStorage =
            new ConcurrentSkipListMap<>(comparator);
    private final AtomicLong storageSize = new AtomicLong(0);
    private final Arena arena;
    private final DiskStorage diskStorage;
    private final Path path;
    private final long flushMemorySize;
    private final ReadWriteLock memoryLock = new ReentrantReadWriteLock();
    private final Lock storageLock = new ReentrantLock();

    public InMemoryDao(Config config) throws IOException {
        this.path = config.basePath().resolve("data");
        this.flushMemorySize = config.flushThresholdBytes();
        Files.createDirectories(path);
        arena = Arena.ofShared();
        this.diskStorage = new DiskStorage(StorageUtils.loadOrRecover(path, arena));
    }

    static int compare(MemorySegment memorySegment1, MemorySegment memorySegment2) {
        long mismatch = memorySegment1.mismatch(memorySegment2);
        if (mismatch == -1) {
            return 0;
        }

        if (mismatch == memorySegment1.byteSize()) {
            return -1;
        }

        if (mismatch == memorySegment2.byteSize()) {
            return 1;
        }
        byte b1 = memorySegment1.get(ValueLayout.JAVA_BYTE, mismatch);
        byte b2 = memorySegment2.get(ValueLayout.JAVA_BYTE, mismatch);
        return Byte.compare(b1, b2);
    }

    @Override
    public Iterator<Entry<MemorySegment>> get(MemorySegment from, MemorySegment to) {
        storageLock.lock();
        try {
            return diskStorage.range(getInMemory(from, to), from, to);
        } finally {
            storageLock.unlock();
        }
    }

    private Iterator<Entry<MemorySegment>> getInMemory(MemorySegment from, MemorySegment to) {
        memoryLock.readLock().lock();
        try {
            if (from == null && to == null) {
                return memoryStorage.values().iterator();
            }
            if (from == null) {
                return memoryStorage.headMap(to).values().iterator();
            }
            if (to == null) {
                return memoryStorage.tailMap(from).values().iterator();
            }
            return memoryStorage.subMap(from, to).values().iterator();
        } finally {
            memoryLock.readLock().unlock();
        }
    }

    @Override
    public void upsert(Entry<MemorySegment> entry) {
        memoryLock.writeLock().lock();
        try {
            storageSize.addAndGet(entry.key().byteSize() + (entry.value() == null ? 0 : entry.value().byteSize()));
            memoryStorage.put(entry.key(), entry);
        } finally {
            memoryLock.writeLock().unlock();
        }
    }

    @Override
    public Entry<MemorySegment> get(MemorySegment key) {
        memoryLock.readLock().lock();
        try {
            Entry<MemorySegment> entry = memoryStorage.get(key);
            if (entry != null) {
                if (entry.value() == null) {
                    return null;
                }
                return entry;
            }
        } finally {
            memoryLock.readLock().unlock();
        }

        storageLock.lock();
        try {
            Iterator<Entry<MemorySegment>> iterator = diskStorage.range(Collections.emptyIterator(), key, null);

            if (!iterator.hasNext()) {
                return null;
            }
            Entry<MemorySegment> next = iterator.next();
            if (compare(next.key(), key) == 0) {
                return next;
            }
        } finally {
            storageLock.unlock();
        }

        return null;
    }

    @Override
    public void compact() throws IOException {
        memoryLock.writeLock().lock();
        try {
            storageLock.lock();
            try {
                if (memoryStorage.isEmpty() && diskStorage.getTotalFiles() <= 1) {
                    return;
                }
                StorageUtils.compact(path, this::all);
            } finally {
                storageLock.unlock();
            }
        } finally {
            memoryLock.writeLock().unlock();
        }
    }

    @Override
    public void flush() throws IOException {
        memoryLock.writeLock().lock();
        try {
            storageLock.lock();
            try {
                if (storageSize.get() < flushMemorySize) {
                    return;
                }

                StorageUtils.save(path, memoryStorage.values());
                storageSize.set(0);
            } finally {
                storageLock.unlock();
            }
        } finally {
            memoryLock.writeLock().unlock();
        }
    }

    @Override
    public void close() throws IOException {
        if (!arena.scope().isAlive()) {
            return;
        }
        memoryLock.writeLock().lock();
        try {
            storageLock.lock();
            try {
                arena.close();
                if (!memoryStorage.isEmpty()) {
                    StorageUtils.save(path, memoryStorage.values());
                }
            } finally {
                storageLock.unlock();
            }
        } finally {
            memoryLock.writeLock().unlock();
        }
    }
}
