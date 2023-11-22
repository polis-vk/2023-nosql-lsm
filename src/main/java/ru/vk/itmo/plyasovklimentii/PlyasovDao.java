package ru.vk.itmo.plyasovklimentii;

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
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class PlyasovDao implements Dao<MemorySegment, Entry<MemorySegment>> {

    private final Comparator<MemorySegment> comparator = PlyasovDao::compare;
    private final Arena arena;
    private final DiskStorage diskStorage;
    private final Path path;
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    private final ExecutorService executorService = Executors.newSingleThreadExecutor();
    private NavigableMap<MemorySegment, Entry<MemorySegment>> currentSSTable = new ConcurrentSkipListMap<>(comparator);
    private NavigableMap<MemorySegment, Entry<MemorySegment>> flushingSSTable = new ConcurrentSkipListMap<>(comparator);
    private final AtomicLong currentSSTableSize = new AtomicLong();

    private final long flushThresholdBytes;
    private final AtomicBoolean isFlushInProgress = new AtomicBoolean(false);
    private final AtomicBoolean isCompactInProgress = new AtomicBoolean(false);

    public PlyasovDao(Config config) throws IOException {
        this.flushThresholdBytes = config.flushThresholdBytes();
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
        List<Iterator<Entry<MemorySegment>>> iterators = new ArrayList<>();
        iterators.addAll(List.of(getInMemory(currentSSTable, from, to)));
        if (isFlushInProgress.get() && flushingSSTable != null) {
            iterators.addFirst(getInMemory(flushingSSTable, from, to));
        }
        return diskStorage.range(iterators, from, to);
    }

    private Iterator<Entry<MemorySegment>> getInMemory(
                    NavigableMap<MemorySegment,
                    Entry<MemorySegment>> storage,
                    MemorySegment from,
                    MemorySegment to) {
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
    public Entry<MemorySegment> get(MemorySegment key) {
        Entry<MemorySegment> currentEntry = currentSSTable.get(key);
        if (currentEntry != null) {
            return currentEntry.value() == null ? null : currentEntry;
        }

        if (isFlushInProgress.get()) {
            Entry<MemorySegment> flushingEntry = flushingSSTable.get(key);
            if (flushingEntry != null) {
                return flushingEntry.value() == null ? null : flushingEntry;
            }
        }

        Iterator<Entry<MemorySegment>> iterator = diskStorage.range(List.of(Collections.emptyIterator()), key, null);
        if (iterator.hasNext()) {
            Entry<MemorySegment> next = iterator.next();
            if (compare(next.key(), key) == 0) {
                return next;
            }
        }
        return null;
    }

    @Override
    public void upsert(Entry<MemorySegment> entry) {
        if (flushingSSTable != null && currentSSTableSize.get() >= flushThresholdBytes) {
            throw new IllegalStateException("Flush in progress, operation cannot proceed");
        }

        long sizeDifference = updateSSTableAndGetSizeDifference(entry);

        if (sizeDifference > 0 && currentSSTableSize.get() >= flushThresholdBytes) {
            try {
                flush();
            } catch (IOException e) {
                throw new UncheckedIOException("Failed to execute flush", e);
            }
        }
    }

    private long updateSSTableAndGetSizeDifference(Entry<MemorySegment> entry) {
        lock.writeLock().lock();
        try {
            Entry<MemorySegment> previousEntry = currentSSTable.put(entry.key(), entry);
            long newSize = calculateEntrySize(entry);
            long oldSize = previousEntry == null ? 0 : calculateEntrySize(previousEntry);
            long sizeDifference = newSize - oldSize;
            currentSSTableSize.addAndGet(sizeDifference);

            return sizeDifference;
        } finally {
            lock.writeLock().unlock();
        }
    }

    private long calculateEntrySize(Entry<MemorySegment> entry) {
        long keySize = entry.key().byteSize();
        long valueSize = entry.value() == null ? Long.BYTES : entry.value().byteSize();
        return keySize + valueSize;
    }

    @Override
    public synchronized void flush() throws IOException {
        if (isFlushInProgress.get() || currentSSTable.isEmpty()) {
            return;
        }

        isFlushInProgress.set(true);
        lock.writeLock().lock();
        try {
            flushingSSTable = currentSSTable;
            currentSSTable = new ConcurrentSkipListMap<>(comparator);
            currentSSTableSize.set(0);
        } finally {
            lock.writeLock().unlock();
        }

        executorService.execute(this::flushToDisk);
    }

    private void flushToDisk() {
        try {
            diskStorage.saveNextSSTable(path, flushingSSTable.values(), arena);
            flushingSSTable = null;
            isFlushInProgress.set(false);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public synchronized void compact() {
        if (isCompactInProgress.get()) {
            return;
        }
        isCompactInProgress.set(true);
        executorService.execute(this::compactOnDisk);

    }

    private void compactOnDisk() {
        try {
            diskStorage.compact(path, this::all);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } finally {
            isCompactInProgress.set(false);
        }
    }

    @Override
    public synchronized void close() throws IOException {
        try {
            if (!arena.scope().isAlive()) {
                return;
            }
            flush();
            executorService.shutdown();
            if (!executorService.awaitTermination(5, TimeUnit.SECONDS)) {
                throw new IOException("Executor did not terminate");
            }
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } finally {
            if (arena.scope().isAlive()) {
                arena.close();
            }
        }
    }

}
