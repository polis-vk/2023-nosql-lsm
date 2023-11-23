package ru.vk.itmo.pelogeikomakar;

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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class ConcurrentDao implements Dao<MemorySegment, Entry<MemorySegment>> {

    private final Comparator<MemorySegment> comparator = ConcurrentDao::compare;
    private final MemoryStorage storage = new MemoryStorage(comparator);
    private final Arena arena;
    private final DiskStorage diskStorage;
    private final Path path;
    private final long flushThresholdBytes;
    private final Lock compactLock = new ReentrantLock();
    private final ReadWriteLock flushLock = new ReentrantReadWriteLock();
    private final Lock flushReadLock = flushLock.readLock();
    private final Lock flushWriteLock = flushLock.writeLock();
    private Thread threadFlush;
    private Thread threadCompact;
    private final AtomicBoolean isClosed = new AtomicBoolean(false);

    public ConcurrentDao(Config config) throws IOException {
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
        flushReadLock.lock();
        try {
            return diskStorage.range(storage.getInMemory(from, to), from, to);
        } finally {
            flushReadLock.unlock();
        }
    }

    @Override
    public Entry<MemorySegment> get(MemorySegment key) {
        flushReadLock.lock();
        try {
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
        } finally {
            flushReadLock.unlock();
        }
    }

    @Override
    public void upsert(Entry<MemorySegment> entry) {
        boolean flushStatus = threadFlush != null && threadFlush.isAlive();
        if (flushStatus) {
            flushWriteLock.lock();
            try {
                if (storage.newIfput(entry.key(), entry) > flushThresholdBytes) {
                    throw new FlushException("Wait until flush ends");
                }
                storage.put(entry.key(), entry, storage.newIfput(entry.key(), entry));
            } finally {
                flushWriteLock.unlock();
            }
        } else {

            flushWriteLock.lock();
            try {
                flushStatus = threadFlush != null && threadFlush.isAlive();
                storage.put(entry.key(), entry, storage.newIfput(entry.key(), entry));

                long memorySize = storage.byteSize();
                if (memorySize >= flushThresholdBytes && !flushStatus) {
                    flush(flushStatus);
                } else if (flushStatus) {
                    throw new FlushException("Wait until flush ends");
                }
            } finally {
                flushWriteLock.unlock();
            }
        }
    }

    @Override
    public void compact() throws IOException {
        compactLock.lock();
        try {
            if (threadCompact != null && threadCompact.isAlive()) {
                return;
            }
            threadCompact = new Thread(() -> {
                try {
                    diskStorage.compact(path, arena);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });
            threadCompact.start();
        } finally {
            compactLock.unlock();
        }
    }

    /**
     * method have to be called with {@link #flushWriteLock flushWriteLock.lock()}
     *
     * @param isFlushing - true if flush has been started. will be set to false after flush ends
     */
    private void flush(boolean isFlushing) {
        if (isFlushing) {
            return;
        }

        var values = storage.prepareFlash();
        // we need to do it before flush start
        diskStorage.getFlushingValues().setRelease(values);
        
        threadFlush = new Thread(() -> {
            try {
                diskStorage.saveNextSSTable(path, values, arena);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
        threadFlush.start();
    }

    @Override
    public void flush() throws IOException {
        boolean isFlushing = threadFlush != null && threadFlush.isAlive();
        flushWriteLock.lock();
        try {
            flush(isFlushing);
        } finally {
            flushWriteLock.unlock();
        }
    }

    @Override
    public void close() throws IOException {
        if (!isClosed.compareAndSet(false, true)) {
            return;
        }
        flushWriteLock.lock();
        try {
            if (!arena.scope().isAlive()) {
                return;
            }

            if (threadCompact != null && threadCompact.isAlive()) {
                try {
                    threadCompact.join();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }

            if (threadFlush != null && threadFlush.isAlive()) {
                try {
                    threadFlush.join();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }

            arena.close();

        } finally {
            if (!storage.isEmpty()) {
                diskStorage.saveNextSSTable(path, storage.prepareFlash(), Arena.ofShared());
            }
            flushWriteLock.unlock();
        }
    }
}
