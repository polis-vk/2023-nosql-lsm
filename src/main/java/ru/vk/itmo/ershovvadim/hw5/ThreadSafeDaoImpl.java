package ru.vk.itmo.ershovvadim.hw5;

import ru.vk.itmo.Config;
import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class ThreadSafeDaoImpl implements Dao<MemorySegment, Entry<MemorySegment>> {

    private final Comparator<MemorySegment> comparator = ThreadSafeDaoImpl::compare;
    private NavigableMap<MemorySegment, Entry<MemorySegment>> storage = new ConcurrentSkipListMap<>(comparator);
    private final Arena arena;
    private final DiskStorage diskStorage;
    private final Path path;
    private final Path compactPath;
    private final long flushThresholdBytes;

    private final ReentrantReadWriteLock rwl = new ReentrantReadWriteLock();
    private final Lock w = rwl.writeLock();

//    private final ExecutorService executorService = Executors.newSingleThreadExecutor();

    private long currentBytes = 0;

    public ThreadSafeDaoImpl(Config config) throws IOException {
        this.flushThresholdBytes = config.flushThresholdBytes();
        this.path = config.basePath().resolve("data");
        this.compactPath = config.basePath().resolve("compact");
        Files.createDirectories(path);

        if (Files.exists(compactPath)) {
            DiskStorage.deleteFiles(compactPath);
        }

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
        if (currentBytes > flushThresholdBytes) {
            flush();
        }
        currentBytes += entry.key().byteSize();
        if (entry.value() != null) {
            currentBytes += entry.value().byteSize();
        }

        storage.put(entry.key(), entry);
    }

    @Override
    public void flush() {
        w.lock();
        try {
            DiskStorage.save(path, storage.values());
            currentBytes = 0;
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            w.unlock();
        }
    }

    @Override
    public Entry<MemorySegment> get(MemorySegment key) {
        Entry<MemorySegment> entry = storage.get(key);
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
        return null;
    }

    @Override
    public void compact() throws IOException {
        w.lock();
        try {
            Files.createDirectories(compactPath);
            DiskStorage.save(compactPath, this::all);
            DiskStorage.deleteFiles(path);
            Files.move(compactPath, path, StandardCopyOption.ATOMIC_MOVE);
            storage = new ConcurrentSkipListMap<>(comparator);
        } finally {
            w.unlock();
        }
    }

    @Override
    public void close() throws IOException {
        w.lock();
        try {
            if (!arena.scope().isAlive()) {
                return;
            }

            arena.close();

            DiskStorage.save(path, storage.values());
        } finally {
            w.unlock();
        }
    }
}
