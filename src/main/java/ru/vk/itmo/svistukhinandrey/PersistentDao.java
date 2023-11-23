package ru.vk.itmo.svistukhinandrey;

import ru.vk.itmo.Config;
import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Iterator;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class PersistentDao implements Dao<MemorySegment, Entry<MemorySegment>> {

    private final StorageState storageState;
    private final Arena arena;
    private final DiskStorage diskStorage;
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    private final ExecutorService executor = Executors.newSingleThreadExecutor();
    private final Path dataPath;
    private final Path compactPath;
    private final long flushThresholdBytes;

    public PersistentDao(Config config) throws IOException {
        dataPath = config.basePath().resolve("data");
        compactPath = config.basePath().resolve("compact_temp");
        flushThresholdBytes = config.flushThresholdBytes();

        Files.createDirectories(dataPath);

        arena = Arena.ofShared();

        this.diskStorage = new DiskStorage(DiskStorage.loadOrRecover(dataPath, arena));
        this.storageState = StorageState.initStorageState();
    }

    @Override
    public Iterator<Entry<MemorySegment>> get(MemorySegment from, MemorySegment to) {
        return diskStorage.range(storageState, from, to);
    }

    @Override
    public void upsert(Entry<MemorySegment> entry) {
        if (storageState.getActiveSSTable().getStorageSize() >= flushThresholdBytes
                && storageState.getFlushingSSTable() != null
        ) {
            throw new IllegalStateException("SSTable is full. Wait until flush.");
        }

        lock.writeLock().lock();
        try {
            storageState.getActiveSSTable().upsert(entry);
        } finally {
            lock.writeLock().unlock();
        }

        try {
            if (storageState.getActiveSSTable().getStorageSize() >= flushThresholdBytes) {
                flush();
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public Entry<MemorySegment> get(MemorySegment key) {
        Entry<MemorySegment> entry = storageState.getActiveSSTable().get(key);
        if (entry != null) {
            if (entry.value() == null) {
                return null;
            }
            return entry;
        }

        return diskStorage.getFromDisk(key);
    }

    @Override
    public synchronized void compact() {
        executor.execute(() -> {
            try {
                Iterable<Entry<MemorySegment>> compactValues = () -> diskStorage.rangeFromDisk(null, null);
                if (compactValues.iterator().hasNext()) {
                    Files.createDirectories(compactPath);
                    DiskStorage.save(compactPath, compactValues);
                    DiskStorage.deleteObsoleteData(dataPath);
                    Files.move(compactPath, dataPath, StandardCopyOption.ATOMIC_MOVE);
                    diskStorage.mapSSTableAfterCompaction(dataPath, arena);
                }
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        });
    }

    @Override
    public synchronized void flush() throws IOException {
        executor.execute(() -> {
            if (!storageState.isReadyForFlush()) {
                return;
            }

            lock.writeLock().lock();
            try {
                storageState.prepareStorageForFlush();
                Iterable<Entry<MemorySegment>> valuesToFlush = () -> storageState.getFlushingSSTable().getAll();
                String filename = DiskStorage.save(dataPath, valuesToFlush);
                storageState.removeFlushingSSTable();
                diskStorage.mapSSTableAfterFlush(dataPath, filename, arena);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            } finally {
                lock.writeLock().unlock();
            }
        });
    }

    @Override
    public void close() throws IOException {
        if (!arena.scope().isAlive()) {
            return;
        }

        try {
            flush();
            executor.shutdown();
            try {
                executor.awaitTermination(5, TimeUnit.MINUTES);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        } finally {
            if (storageState.isReadyForFlush()) {
                storageState.prepareStorageForFlush();
            }
            arena.close();
        }
    }
}
