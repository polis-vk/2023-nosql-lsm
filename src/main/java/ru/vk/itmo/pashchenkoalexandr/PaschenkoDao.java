package ru.vk.itmo.pashchenkoalexandr;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import ru.vk.itmo.Config;
import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;

public class PaschenkoDao implements Dao<MemorySegment, Entry<MemorySegment>> {
    private final Config config;
    private volatile MemTable storage;
    private volatile MemTable toFlush;
    private final Arena arena;
    private volatile DiskStorage diskStorage;
    private final Path path;
    private final ExecutorService compactionExecutor = Executors.newFixedThreadPool(2);
    private volatile Future<?> compactionFuture;
    private volatile Future<?> flushFuture;
    private final ReadWriteLock compactionLock = new ReentrantReadWriteLock();
    private final ReadWriteLock upsertLock = new ReentrantReadWriteLock();

    public PaschenkoDao(Config config) throws IOException {
        this.config = config;
        this.storage = new MemTable(config.flushThresholdBytes());
        this.path = config.basePath().resolve("data");
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
        return diskStorage.range(getInMemories(from, to), from, to);
    }

    private List<Iterator<Entry<MemorySegment>>> getInMemories(MemorySegment from, MemorySegment to) {
        MemTable mem1 = storage;
        MemTable mem2 = toFlush;

        List<Iterator<Entry<MemorySegment>>> res = new ArrayList<>();
        if (mem2 != null) {
            res.add(mem2.getInMemory(from, to));
        }
        res.add(mem1.getInMemory(from, to));
        return res;
    }

    @Override
    public void upsert(Entry<MemorySegment> entry) {
        upsertLock.readLock().lock();
        try {
            if (storage.upsert(entry)) {
                return;
            }
        } finally {
            upsertLock.readLock().unlock();
        }
        tryToFlush();
        upsertLock.readLock().lock();
        try {
            if (storage.upsert(entry)) {
                return;
            } else {
                throw new RuntimeException("To many upserts, current storage size = " + storage.getSize() + " max size: " + config.flushThresholdBytes() + " entry: " + MemTable.entrySize(entry));
            }
        } finally {
            upsertLock.readLock().unlock();
        }
    }

    private boolean tryToFlush() {
        upsertLock.writeLock().lock();
        try {
            if (toFlush == null) {
                toFlush = storage;
                storage = new MemTable(config.flushThresholdBytes());
                flushFuture = compactionExecutor.submit(() -> {
                    try {
                        doFlush();
                    } catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                });
                return true;
            } else {
                return false;
            }
        } finally {
            upsertLock.writeLock().unlock();
        }
    }

    @Override
    public void flush() throws IOException {
        tryToFlush();
    }

    private void doFlush() throws IOException {
        long start = System.nanoTime();
        System.out.println(new Timestamp(System.currentTimeMillis()) + " Start flush");
        compactionLock.writeLock().lock();
        try {
            if (toFlush == null) {
                return;
            }
            if (!toFlush.getStorage().isEmpty()) {
                System.out.println("Flush size " + toFlush.getStorage().values().size());
                DiskStorage.saveNextSSTable(path, toFlush.getStorage().values());
            }
            this.diskStorage = new DiskStorage(DiskStorage.loadOrRecover(path, arena));
            toFlush = null;
        } finally {
            compactionLock.writeLock().unlock();
            System.out.println("Flush Total time: " + TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start));
        }
    }

    @Override
    public Entry<MemorySegment> get(MemorySegment key) {
        Entry<MemorySegment> entry = storage.getStorage().get(key);
        if (entry != null) {
            if (entry.value() == null) {
                return null;
            }
            return entry;
        }

        MemTable tmp = toFlush;
        if (tmp != null) {
            entry = tmp.getStorage().get(key);
            if (entry != null) {
                if (entry.value() == null) {
                    return null;
                }
                return entry;
            }
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

    @Override
    public void compact() throws IOException {
        if (compactionFuture != null && !compactionFuture.isDone()) {
            compactionFuture.cancel(false);
        }
        compactionFuture = compactionExecutor.submit(() -> {
            try {
                long start = System.nanoTime();
                System.out.println(new Timestamp(System.currentTimeMillis()) + " Start compact");
                compactionLock.writeLock().lock();
                try {
                    DiskStorage.compact(path, () -> diskStorage.range(null, null));
                    this.diskStorage = new DiskStorage(DiskStorage.loadOrRecover(path, arena));
                } finally {
                    compactionLock.writeLock().unlock();
                    System.out.println("Compact Total time: " + TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start));
                }
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        });
    }

    @Override
    public void close() throws IOException {
        if (!arena.scope().isAlive()) {
            return;
        }
        try {
            if (compactionFuture != null) {
                compactionFuture.get();
            }
            if (flushFuture != null) {
                flushFuture.get();
            }
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        }
        compactionLock.writeLock().lock();
        try {
            gracefulShutdown(compactionExecutor);
            arena.close();
            if (!storage.getStorage().isEmpty()) {
                DiskStorage.saveNextSSTable(path, storage.getStorage().values());
            }
        } finally {
            compactionLock.writeLock().unlock();
        }
    }

    private void gracefulShutdown(ExecutorService executor) {
        executor.shutdown();
        try {
            executor.awaitTermination(1, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        executor.shutdownNow();
    }
}
