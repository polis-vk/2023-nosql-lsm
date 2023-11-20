package ru.vk.itmo.osokindmitry;

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
import java.util.Iterator;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class PersistentDao implements Dao<MemorySegment, Entry<MemorySegment>> {

    private final MemTable memTable;
    private final Arena arena;
    private final Path path;
    private final DiskStorage diskStorage;
    private final long thresholdBytes;
    private final ReentrantReadWriteLock rwLock;
    private final ExecutorService compactionExecutor;
    private ExecutorService flushExecutor;
    private final ExecutorService finalizer;
    private Future<?> autoFlushing;
//    private Future<?> flushingTask;
    private final AtomicBoolean shuttingDown;

    public PersistentDao(Config config) throws IOException {
        path = config.basePath().resolve("data");
        Files.createDirectories(path);

        thresholdBytes = config.flushThresholdBytes();
        arena = Arena.ofShared();
        rwLock = new ReentrantReadWriteLock();

        memTable = new MemTable(new ConcurrentSkipListMap<>(PersistentDao::compare), thresholdBytes);

        compactionExecutor = Executors.newSingleThreadExecutor();
        flushExecutor = Executors.newFixedThreadPool(2);
        finalizer = Executors.newSingleThreadExecutor();

        autoFlushing = flushExecutor.submit(new AutoFlusher());
        shuttingDown = new AtomicBoolean(false);

        this.diskStorage = new DiskStorage(Utils.loadOrRecover(path), arena);
    }

    @Override
    public Entry<MemorySegment> get(MemorySegment key) {
        rwLock.readLock().lock();
        try {
            Entry<MemorySegment> entry = memTable.get(key);
            if (entry != null) {
                if (entry.value() == null) {
                    return null;
                }
                return entry;
            }
        } finally {
            rwLock.readLock().unlock();
        }

        Iterator<Entry<MemorySegment>> iterator = diskStorage.range(Collections.emptyList(), key, null);

        if (!iterator.hasNext()) {
            return null;
        }
        Entry<MemorySegment> next = iterator.next();
        if (compare(next.key(), key) == 0) {
            return next;
        }
        return null;
    }

    static int compare(MemorySegment segment1, MemorySegment segment2) {
        long offset = segment1.mismatch(segment2);
        if (offset == -1) {
            return 0;
        } else if (offset == segment1.byteSize()) {
            return -1;
        } else if (offset == segment2.byteSize()) {
            return 1;
        }
        byte b1 = segment1.get(ValueLayout.JAVA_BYTE, offset);
        byte b2 = segment2.get(ValueLayout.JAVA_BYTE, offset);
        return Byte.compare(b1, b2);
    }

    @Override
    public Iterator<Entry<MemorySegment>> get(MemorySegment from, MemorySegment to) {
        rwLock.readLock().lock();
        try {
            return diskStorage.range(memTable.get(from, to), from, to);
        } finally {
            rwLock.readLock().unlock();
        }
    }

    @Override
    public void upsert(Entry<MemorySegment> entry) throws IllegalStateException {
        rwLock.writeLock().lock();
        try {
            memTable.put(entry.key(), entry);
        } finally {
            rwLock.writeLock().unlock();
        }
    }

    @Override
    public void flush() throws IOException {
        rwLock.readLock().lock();
        try {
            if (!memTable.getTable().isEmpty()) {
//                flushingTask = flushExecutor.submit(new FlushingTask<>());
                flushExecutor.execute(new FlushingTask<>());
            }
        } finally {
            rwLock.readLock().unlock();
        }
    }

    @Override
    public void compact() {
        rwLock.readLock().lock();
        try {
            compactionExecutor.execute(() -> {
                try {
                    diskStorage.compact(path);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });
        } finally {
            rwLock.readLock().unlock();
        }
    }

    @Override
    public void close() throws IOException {
        if (!arena.scope().isAlive()) {
            return;
        }
        shuttingDown.set(true);
        autoFlushing.cancel(true);
        compactionExecutor.close();
        flushExecutor.close();
        arena.close();
        flushExecutor = finalizer;
        flush();
        finalizer.close();
    }

    private class FlushingTask<V> extends FutureTask<V> {
        public FlushingTask() {
            super(() -> {
                rwLock.writeLock().lock();
                try {
                    memTable.set(new ConcurrentSkipListMap<>(PersistentDao::compare));
                    memTable.setIsFlushing(true);
                    diskStorage.save(path, memTable.getFlushingTable().values());
                    memTable.setIsFlushing(false);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                } finally {
                    rwLock.writeLock().unlock();
                }
                return null;
            });
        }
    }

    private class AutoFlusher implements Runnable {

        @Override
        public void run() {
            while (!shuttingDown.get()) {
                rwLock.readLock().lock();
                try {
                    if (memTable.size() > thresholdBytes) {
                        flush();
                    }
                } catch (IOException e) {
                    throw new RuntimeException(e);
                } finally {
                    rwLock.readLock().unlock();
                }
            }
        }
    }

}
