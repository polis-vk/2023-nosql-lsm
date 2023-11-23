package ru.vk.itmo.osipovdaniil;

import ru.vk.itmo.Config;
import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Iterator;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class InMemoryDao implements Dao<MemorySegment, Entry<MemorySegment>> {

    private static final String DATA = "data";

    private final Path path;

    private final Arena arena;

    private final long flushThresholdBytes;

    private final Lock filesLock;

    private final ReadWriteLock mapLock;

    final AtomicBoolean isFlushing;

    private final AtomicLong mapSize;

    private final DiskStorage diskStorage;
    private final ConcurrentNavigableMap<MemorySegment, Entry<MemorySegment>> memorySegmentMap
            = new ConcurrentSkipListMap<>(Utils::compareMemorySegments);

    public InMemoryDao(final Config config) throws IOException {
        this.path = config.basePath().resolve(DATA);
        Files.createDirectories(path);
        this.arena = Arena.ofShared();
        this.flushThresholdBytes = config.flushThresholdBytes();
        this.filesLock = new ReentrantLock();
        this.mapLock = new ReentrantReadWriteLock();
        this.isFlushing = new AtomicBoolean(false);
        this.mapSize = new AtomicLong(0);
        this.diskStorage = new DiskStorage(DiskStorageUtils.loadOrRecover(path, arena));
    }

    /**
     * Compacts data (no-op by default).
     */
    @Override
    public void compact() throws IOException {
        filesLock.lock();
        try {
            DiskStorageUtils.compact(path, () -> getOnDisk(null, null));
        } finally {
            filesLock.unlock();
        }
    }

    /**
     * Returns ordered iterator of entries with keys between from (inclusive) and to (exclusive).
     *
     * @param from lower bound of range (inclusive)
     * @param to   upper bound of range (exclusive)
     * @return entries [from;to)
     */
    @Override
    public Iterator<Entry<MemorySegment>> get(final MemorySegment from, final MemorySegment to) {
        filesLock.lock();
        try {
            return diskStorage.range(getInMemory(from, to), from, to);
        } finally {
            filesLock.unlock();
        }
    }

    public Iterator<Entry<MemorySegment>> getOnDisk(final MemorySegment from, final MemorySegment to) {
        filesLock.lock();
        try {
            return diskStorage.range(Collections.emptyIterator(), from, to);
        } finally {
            filesLock.unlock();
        }
    }

    private Iterator<Entry<MemorySegment>> getInMemory(final MemorySegment from, final MemorySegment to) {
        mapLock.readLock().lock();
        try {
            if (from == null && to == null) {
                return memorySegmentMap.values().iterator();
            }
            if (from == null) {
                return memorySegmentMap.headMap(to).values().iterator();
            }
            if (to == null) {
                return memorySegmentMap.tailMap(from).values().iterator();
            }
            return memorySegmentMap.subMap(from, to).values().iterator();
        } finally {
            mapLock.readLock().unlock();
        }
    }

    /**
     * Returns entry by key. Note: default implementation is far from optimal.
     *
     * @param key entry`s key
     * @return entry
     */
    @Override
    public Entry<MemorySegment> get(final MemorySegment key) {
        mapLock.readLock().lock();
        try {
            final Entry<MemorySegment> entry = memorySegmentMap.get(key);
            if (entry != null) {
                if (entry.value() == null) {
                    return null;
                }
                return entry;
            }
        } finally {
            mapLock.readLock().unlock();
        }
        filesLock.lock();
        try {
            final Iterator<Entry<MemorySegment>> iterator = diskStorage.range(
                    Collections.emptyIterator(), key, null);
            if (!iterator.hasNext()) {
                return null;
            }
            Entry<MemorySegment> next = iterator.next();
            if (next.key().mismatch(key) == -1) {
                return next;
            }
            return null;
        } finally {
            filesLock.unlock();
        }
    }

    /**
     * Inserts of replaces entry.
     *
     * @param entry element to upsert
     */
    @Override
    public void upsert(final Entry<MemorySegment> entry) {
        mapLock.writeLock().lock();
        try {
            final long valSize = entry.value() == null ? 0 : entry.value().byteSize();
            mapSize.addAndGet(entry.key().byteSize() + valSize);
            if (mapSize.get() >= flushThresholdBytes) {
                if (isFlushing.get()) {
                    throw new FlushWhileFlushingException();
                }
                memorySegmentMap.put(entry.key(), entry);
                try {
                    flush();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        } finally {
            mapLock.writeLock().unlock();
        }
    }

    /**
     * Persists data (no-op by default).
     */
    @Override
    public void flush() throws IOException {
        if (isFlushing.get()) {
            throw new FlushWhileFlushingException();
        }
        isFlushing.set(true);
        if (!memorySegmentMap.isEmpty()) {
            DiskStorageUtils.save(path, memorySegmentMap.values());
            memorySegmentMap.clear();
            mapSize.set(0);
        }
        isFlushing.set(false);
    }

    @Override
    public void close() throws IOException {
        while (isFlushing.get() || DiskStorageUtils.isCompacting.get()) {
        }
        if (!arena.scope().isAlive()) {
            return;
        }
        arena.close();
        flush();
    }
}
