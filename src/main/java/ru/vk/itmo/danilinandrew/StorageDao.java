package ru.vk.itmo.danilinandrew;

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
import java.util.NavigableMap;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class StorageDao implements Dao<MemorySegment, Entry<MemorySegment>> {
    private final Arena arena;
    private MemTable memTable;
    private final DiskStorage diskStorage;
    private final Path path;
    private final Config config;
    private final ReadWriteLock upsertLock = new ReentrantReadWriteLock();
    private final ExecutorService flushingExecutorService = Executors.newSingleThreadExecutor();
    private final ExecutorService compactExecutorService = Executors.newSingleThreadExecutor();
    private final AtomicBoolean isFlushing = new AtomicBoolean();

    public StorageDao(Config config) throws IOException {
        this.path = config.basePath().resolve("data");
        Files.createDirectories(path);
        arena = Arena.ofShared();

        this.config = config;
        this.memTable = new MemTable(config.flushThresholdBytes());
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
        return diskStorage.range(memTable.get(from, to), from, to);
    }

    @Override
    public void upsert(Entry<MemorySegment> entry) {
        upsertLock.writeLock().lock();
        try {
            boolean needToFlush = memTable.upsert(entry, isFlushing);

            if (needToFlush) {
                Future<?> future = flushingExecutorService.submit(() -> {
                    try {
                        this.memTable = new MemTable(
                                this.memTable.getStorage(),
                                this.memTable.getFlushingStorage(),
                                config.flushThresholdBytes()
                        );

                        DiskStorage.saveNextSSTable(
                                path,
                                this.memTable.getFlushingStorage().values()
                        );

                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    } finally {
                        isFlushing.set(false);
                    }
                });

                future.get();
            }
        } catch (ExecutionException | InterruptedException e) {
            throw new RuntimeException(e);
        } finally {
            upsertLock.writeLock().unlock();
        }
    }

    @Override
    public Entry<MemorySegment> get(MemorySegment key) {
        Entry<MemorySegment> entry = memTable.get(key);
        if (entry != null) {
            if (entry.value() == null) {
                return null;
            }
            return entry;
        }

        if (!memTable.isEmpty()) {
            entry = memTable.getFlushingStorage().get(key);
            if (entry != null) {
                return entry;
            }
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
        diskStorage.lockStorage();
        try {
            Future<?> future = compactExecutorService.submit(() -> {
               try {
                   DiskStorage.compact(path, this::all);
               } catch (IOException e) {
                   throw new RuntimeException(e);
               }
            });

            future.get();

        } catch (ExecutionException e) {
            try {
                throw e.getCause();
            } catch (RuntimeException | IOException | Error r) {
                throw r;
            } catch (Throwable t) {
                throw new RuntimeException(t);
            }
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } finally {
            diskStorage.unlockStorage();
        }
    }

    @Override
    public void close() throws IOException {
        if (!arena.scope().isAlive()) {
            return;
        }

        arena.close();

        flushingExecutorService.shutdown();
        compactExecutorService.shutdown();

        try {
            boolean flushingTerminated;
            boolean compactTerminated;
            do {
                flushingTerminated = flushingExecutorService.awaitTermination(10, TimeUnit.HOURS);
            } while (!flushingTerminated);

            do {
                compactTerminated = compactExecutorService.awaitTermination(10, TimeUnit.HOURS);
            } while (!compactTerminated);
        } catch (InterruptedException e) {
            throw new IllegalStateException();
        }

        if (!memTable.isEmpty()) {
            DiskStorage.saveNextSSTable(
                    path,
                    memTable.values()
            );
        }
    }
}
