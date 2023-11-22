package ru.vk.itmo.pelogeikomakar;

import ru.vk.itmo.Config;
import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;
import ru.vk.itmo.kobyzhevaleksandr.ApplicationException;

import java.io.IOException;
import java.io.Reader;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
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
    private final AtomicBoolean atomicCompact = new AtomicBoolean(false);
    private final Lock compactLock = new ReentrantLock();
    private final ReadWriteLock flushLock = new ReentrantReadWriteLock();
    private final Lock flushReadLock = flushLock.readLock();
    private final Lock flushWriteLock = flushLock.writeLock();
    private Thread threadFlush;
    private Thread threadCompact;
    private volatile boolean isClosed;

    public ConcurrentDao(Config config) throws IOException {
        isClosed = false;
        this.path = config.basePath().resolve("data");
        this.flushThresholdBytes = config.flushThresholdBytes();
        Files.createDirectories(path);

        arena = Arena.ofShared();

        this.diskStorage = new DiskStorage(DiskStorage.loadOrRecover(path, arena), path , arena);
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

            Iterator<Entry<MemorySegment>> iterator = diskStorage.range(null, key, null);

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
        flushWriteLock.lock();
        try {
            boolean flushStatus = storage.getFlushStatus();
            if (flushStatus) {
                long addingSize = entry.key().byteSize() * 2 + (entry.value() == null ? 0: entry.value().byteSize());
                var entryVal = storage.get(entry.key());
                if (entryVal != null && entryVal.value() != null) {
                    addingSize = addingSize - entryVal.value().byteSize();
                }

                if (storage.byteSize() + addingSize > flushThresholdBytes) {
                    throw new FlushException("Wait until flush ends");
                }
                storage.put(entry.key(), entry);
            } else {

                storage.put(entry.key(), entry);

                long memorySize = storage.byteSize();
                if (memorySize >= flushThresholdBytes) {
                    flush(false);
                }
            }
        } finally {
            flushWriteLock.unlock();
        }
    }

    @Override
    public void compact() throws IOException {
        compactLock.lock();
        try {
            if (threadCompact != null && threadCompact.isAlive())
            {
                return;
            }
            // returns value right before atomic action so we expect false
            boolean result = !atomicCompact.compareAndExchangeAcquire(false, true);
            if (result) {
                threadCompact = new Thread(() -> {
                    try {
                        diskStorage.compact(path);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                });
                threadCompact.start();
                atomicCompact.set(false);
            }
        } finally {
            compactLock.unlock();
        }
    }

    /**
     * method have to be called with {@link #flushWriteLock flushWriteLock.lock()}
     * @param isFlushing - true if flush has been started. will be set to false after flush ends
     */
    private void flush(boolean isFlushing) {
        if (isFlushing) {
            return;
        }

        var values = storage.values();
        var atomicName = storage.getAtomicTableName();
        storage.setFlushStatus(true);
        threadFlush = new Thread(() -> {
            try {
                diskStorage.saveNextSSTable(path, values, storage, atomicName, true);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
        storage.makeNewStorage();
        threadFlush.start();
    }
    @Override
    public void flush() throws IOException {
        flushWriteLock.lock();
        try {
            flush(storage.getFlushStatus());
        } finally {
            flushWriteLock.unlock();
        }
    }

    @Override
    public void close() throws IOException {
        if (isClosed) {
            return;
        }
        flushWriteLock.lock();
        try {
            isClosed = true;
            if (!arena.scope().isAlive()) {
                return;
            }

            if (threadFlush != null && threadFlush.isAlive()) {
                try {
                    threadFlush.join();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }

            if (threadCompact != null && threadCompact.isAlive()) {
                try {
                    threadCompact.join();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }

            arena.close();

        } finally {
            if (!storage.isEmpty()) {
                diskStorage.saveNextSSTable(path, storage.values(), storage, storage.getAtomicTableName(), false);
            }
            flushWriteLock.unlock();
        }
    }
}
