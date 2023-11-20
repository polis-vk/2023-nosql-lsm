package ru.vk.itmo.svistukhinandrey;

import ru.vk.itmo.Config;
import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.file.Files;
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
    private final ConfigWrapper config;
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    private final ExecutorService executor = Executors.newSingleThreadExecutor();

    public PersistentDao(Config config) throws IOException {
        this.config = new ConfigWrapper(
                config.flushThresholdBytes(),
                config.basePath().resolve("data"),
                config.basePath().resolve("compact_temp")
        );

        Files.createDirectories(this.config.getDataPath());
        Files.deleteIfExists(this.config.getCompactTempPath());

        arena = Arena.ofShared();

        this.diskStorage = new DiskStorage(DiskStorage.loadOrRecover(this.config.getDataPath(), arena));
        this.storageState = StorageState.initStorageState();
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
        return diskStorage.range(storageState, from, to);
    }

    @Override
    public void upsert(Entry<MemorySegment> entry) {
        if (storageState.getActiveSSTable().getStorageSize() >= config.getFlushThresholdBytes()
                && storageState.getFlushingSSTable() != null
        ) {
            throw new IllegalStateException("SSTable is full. Wait until flush.");
        }

        lock.readLock().lock();
        try {
            storageState.getActiveSSTable().upsert(entry);
        } finally {
            lock.readLock().unlock();
        }

        try {
            if (storageState.getActiveSSTable().getStorageSize() >= config.getFlushThresholdBytes()) {
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

        Iterator<Entry<MemorySegment>> iterator = diskStorage.rangeFromDisk(key, null);

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
    public synchronized void compact() {
        if (!arena.scope().isAlive()) {
            throw new IllegalStateException("DAO is closed.");
        }

        executor.execute(this::compactOnDisk);
    }

    @Override
    public synchronized void flush() throws IOException {
        if (!storageState.isReadyForFlush()) {
            return;
        }

        storageState.prepareStorageForFlush();
        executor.execute(() -> {
            lock.writeLock().lock();
            try {
                flushToDisk();
            } finally {
                lock.writeLock().unlock();
            }
        });
    }

    @Override
    public synchronized void close() throws IOException {
        if (!arena.scope().isAlive()) {
            return;
        }

        try {
            flush();
            executor.shutdown();
            executor.awaitTermination(5, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            throw new IllegalStateException(e);
        } finally {
            if (storageState.isReadyForFlush()) {
                storageState.prepareStorageForFlush();
                flushToDisk();
            }
            arena.close();
        }
    }

    private void flushToDisk() {
        try {
            Iterable<Entry<MemorySegment>> valuesToFlush = () -> storageState.getFlushingSSTable().getAll();
            String filename = DiskStorage.save(config.getDataPath(), valuesToFlush);
            storageState.removeFlushingSSTable();
            diskStorage.addSegment(config.getDataPath(), filename, arena);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private void compactOnDisk() {
        try {
            Iterable<Entry<MemorySegment>> compactValues = () -> diskStorage.rangeFromDisk(null, null);
            if (compactValues.iterator().hasNext()) {
                Files.createDirectories(config.getCompactTempPath());
                DiskStorage.save(config.getCompactTempPath(), compactValues);
                DiskStorage.deleteObsoleteData(config.getDataPath());
                Files.move(config.getCompactTempPath(), config.getDataPath(), StandardCopyOption.ATOMIC_MOVE);
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
