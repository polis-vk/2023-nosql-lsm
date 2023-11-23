package ru.vk.itmo.khodosovaelena;

import ru.vk.itmo.Config;
import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;

import java.io.IOException;
import java.io.UncheckedIOException;
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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class InMemoryDao implements Dao<MemorySegment, Entry<MemorySegment>> {

    private final Comparator<MemorySegment> comparator = InMemoryDao::compare;
    private NavigableMap<MemorySegment, Entry<MemorySegment>> storage = new ConcurrentSkipListMap<>(comparator);
    private final Arena arena;
    private final DiskStorage diskStorage;
    private final Path path;
    private final long flushThresholdBytes;
    private final ExecutorService executor =
            Executors.newSingleThreadExecutor(r -> new Thread(r, "BgTasksExecutor"));
    private NavigableMap<MemorySegment, Entry<MemorySegment>> flushStorage = new ConcurrentSkipListMap<>(comparator);
    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    public InMemoryDao(Config config) throws IOException {
        this.path = config.basePath().resolve("data");
        this.flushThresholdBytes = config.flushThresholdBytes();
        Files.createDirectories(path);

        arena = Arena.ofShared();

        this.diskStorage = new DiskStorage(DiskStorage.loadOrRecover(path, arena));
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
        if (!arena.scope().isAlive()) throw new IllegalStateException("Arena is closed");
        return diskStorage.range(getInMemory(from, to), from, to);
    }

    private Iterator<Entry<MemorySegment>> getInMemory(MemorySegment from, MemorySegment to) {
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

    @Override
    public void upsert(Entry<MemorySegment> entry) {

        if (storage.size() >= flushThresholdBytes
                && flushStorage != null) {
            throw new IllegalStateException("Flush in progress...");
        }

        lock.readLock().lock();
        try {
            storage.put(entry.key(), entry);
        } finally {
            lock.readLock().unlock();
        }

        storage.put(entry.key(), entry);

    }

    @Override
    public synchronized void flush() throws IOException {
        if (storage.isEmpty() || flushStorage != null) {
            return;
        }

        executor.execute(() -> {
            lock.writeLock().lock();
            try {
                flushStorage = storage;
                storage = new ConcurrentSkipListMap<>(comparator);
            } finally {
                lock.writeLock().unlock();
            }
            synchronized (this) {
                try {
                    DiskStorage.saveNextSSTable(path, flushStorage.values());
                    flushStorage = null;
                } catch (IOException e) {
                    throw new IllegalStateException("Error during flush", e);
                }
            }
        });
    }

    @Override
    public Entry<MemorySegment> get(MemorySegment key) {
        Entry<MemorySegment> entry = storage.get(key);
        synchronized (this) {
            if (entry != null) {
                if (entry.value() == null) {
                    return null;
                }
                return entry;
            }

            Iterator<Entry<MemorySegment>> iterator = diskStorage.range(Collections.emptyIterator(), key, null);

            if (!iterator.hasNext()) {
                return null;
            }
            Entry<MemorySegment> next = iterator.next();
            if (compare(next.key(), key) == 0) {
                return next;
            }
        }
        return null;
    }

    @Override
    public void compact() throws IOException {
        executor.execute(() -> {
            synchronized (this) {
                try {
                    DiskStorage.compact(path, this::all);
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            }
        });
    }

    @Override
    public void close() throws IOException {
        if (!arena.scope().isAlive()) {
            return;
        }

        arena.close();
        //  flush();
        executor.shutdown();

        if (!storage.isEmpty()) {
            flushStorage = storage;
            flush();
        }
        DiskStorage.saveNextSSTable(path, flushStorage.values());
    }

}
