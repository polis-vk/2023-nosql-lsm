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
import java.util.concurrent.*;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class PersistentDao implements Dao<MemorySegment, Entry<MemorySegment>> {

    private ConcurrentNavigableMap<MemorySegment, Entry<MemorySegment>> storage;
    private final Arena arena;
    private final Path path;
    private final DiskStorage diskStorage;
    private final long thresholdBytes;
    private final ReentrantReadWriteLock rwLock;
    private final ExecutorService compactionExecutor;
    private final ExecutorService flushExecutor;
    private final ExecutorService finalizer;
    private Future<?> autoFlushing;
    private Future<?> flushingTask;
    private volatile boolean shuttingDown;
    private long storageSize;

    public PersistentDao(Config config) throws IOException {
        path = config.basePath().resolve("data");
        Files.createDirectories(path);

        thresholdBytes = config.flushThresholdBytes();
        arena = Arena.ofShared();
        storage = new ConcurrentSkipListMap<>(PersistentDao::compare);

        rwLock = new ReentrantReadWriteLock();

        compactionExecutor = Executors.newSingleThreadExecutor();
        flushExecutor = Executors.newFixedThreadPool(2);
        finalizer = Executors.newSingleThreadExecutor();

        autoFlushing = flushExecutor.submit(new AutoFlusher());
        shuttingDown = false;
        storageSize = 0;

        this.diskStorage = new DiskStorage(Utils.loadOrRecover(path), arena);
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
    public Iterator<Entry<MemorySegment>> get(MemorySegment from, MemorySegment to) {
        return diskStorage.range(getInMemory(from, to), from, to);
    }

    @Override
    public void upsert(Entry<MemorySegment> entry) throws IllegalStateException {
        // если флашится первый мем тейбл и заполнен второй, то кидаем исключение
        rwLock.writeLock().lock();
        try {
            if (unableToUpsert(entry)) {
                throw new IllegalStateException();
            }
            storage.put(entry.key(), entry);
            storageSize += entry.key().byteSize() + entry.value().byteSize();
        } finally {
            rwLock.writeLock().unlock();
        }
    }

    @Override
    public void flush() throws IOException {
        if (!storage.isEmpty()) {
            flushingTask = flushExecutor.submit(new FlushingTask<>());
        }
    }

    @Override
    public void compact() {
        autoFlushing = compactionExecutor.submit(() -> {
            try {
                diskStorage.compact(path);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
    }

    @Override
    public void close() throws IOException {
        if (!arena.scope().isAlive()) {
            return;
        }
        shuttingDown = true;
        autoFlushing.cancel(true);
        compactionExecutor.close();
        flushExecutor.close();
        arena.close();
        autoFlushing = finalizer.submit(new FlushingTask<>());
        finalizer.close();
    }

    private boolean unableToUpsert(Entry<MemorySegment> entry) {
        return storageSize + entry.key().byteSize() + entry.value().byteSize() > thresholdBytes
                && !flushingTask.isDone();
    }

    private class FlushingTask<V> extends FutureTask<V> {
        public FlushingTask() {
            super(() -> {
                try {
                    DiskStorage.save(path, storage.values());
                    storage = new ConcurrentSkipListMap<>(PersistentDao::compare);
                    storageSize = 0;
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
                return null;
            });
        }

//        @Override
//        protected void done() {
//            super.done();
//        }

    }

    private class AutoFlusher implements Runnable {

        @Override
        public void run() {
            while (!shuttingDown) {
                if (storageSize > thresholdBytes) {
                    try {
                        flush();
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }
            }
        }
    }

}
