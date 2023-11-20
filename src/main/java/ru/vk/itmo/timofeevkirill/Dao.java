package ru.vk.itmo.timofeevkirill;

import ru.vk.itmo.Config;
import ru.vk.itmo.Entry;

import java.io.IOException;
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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class Dao implements ru.vk.itmo.Dao<MemorySegment, Entry<MemorySegment>> {

    private final Comparator<MemorySegment> comparator = Dao::compare;
    private NavigableMap<MemorySegment, Entry<MemorySegment>> storage = new ConcurrentSkipListMap<>(comparator);
    private final Arena arena;
    private final DiskStorage diskStorage;
    private final Path path;
    private final long flushThresholdBytes;
    private final ExecutorService executorService = Executors.newFixedThreadPool(5);
    private final AtomicLong storageBytesSize = new AtomicLong(0);
    private NavigableMap<MemorySegment, Entry<MemorySegment>> storageBuffer = new ConcurrentSkipListMap<>(comparator);

    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    private Future<?> flushTask;
    private Future<?> compactionTask;

    public Dao(Config config) throws IOException {
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
        List<Iterator<Entry<MemorySegment>>> inMem = new ArrayList<>(List.of(cutInMemory(storage, from, to)));
        if (!(flushTask == null || flushTask.isDone())) {
            inMem.addFirst(cutInMemory(storageBuffer, from, to));
        }
        return diskStorage.range(from, to, inMem);
    }

    private Iterator<Entry<MemorySegment>> cutInMemory(NavigableMap<MemorySegment, Entry<MemorySegment>> toCut,
                                                       MemorySegment from, MemorySegment to) {
        if (from == null && to == null) {
            return toCut.values().iterator();
        }
        if (from == null) {
            return toCut.headMap(to).values().iterator();
        }
        if (to == null) {
            return toCut.tailMap(from).values().iterator();
        }
        return toCut.subMap(from, to).values().iterator();
    }

    @Override
    public void upsert(Entry<MemorySegment> entry) {
        if (!(flushTask == null || flushTask.isDone()) && storageBytesSize.get() >= flushThresholdBytes) {
            throw new IllegalStateException("Previous flush has not yet ended");
        }

        lock.readLock().lock();
        try {
            long valueSize;
            if (entry.value() == null) {
                valueSize = Long.BYTES;
            } else {
                valueSize = entry.value().byteSize();
            }
            Entry<MemorySegment> prev = storage.put(entry.key(), entry);
            if (prev == null) {
                storageBytesSize.addAndGet(entry.key().byteSize() + valueSize);
            } else {
                storageBytesSize.addAndGet(valueSize);
            }
        } finally {
            lock.readLock().unlock();
        }

        if (storageBytesSize.get() >= flushThresholdBytes) {
            try {
                flush();
            } catch (IOException e) {
                throw new IllegalStateException("Auto flush exception", e);
            }
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

        if (storageBuffer != null) {
            Entry<MemorySegment> bufferEntry = storageBuffer.get(key);
            if (bufferEntry != null) {
                if (bufferEntry.value() == null) {
                    return null;
                }
                return bufferEntry;
            }
        }

        Iterator<Entry<MemorySegment>> iterator = diskStorage.range(key, null, List.of(Collections.emptyIterator()));

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
    public synchronized void compact() throws IOException {
        if (!(compactionTask == null || compactionTask.isDone())) {
            return;
        }

        compactionTask = executorService.submit(() -> {
            try {
                DiskStorage.compact(path, this::all);
            } catch (IOException e) {
                throw new IllegalStateException("Can not compact", e);
            }
        });
    }

    @Override
    public synchronized void flush() throws IOException {
        if (!(flushTask == null || flushTask.isDone()) || storage.isEmpty()) {
            return;
        }

        lock.writeLock().lock();
        try {
            storageBuffer = storage;
            storage = new ConcurrentSkipListMap<>(comparator);
            storageBytesSize.set(0);
        } finally {
            lock.writeLock().unlock();
        }

        flushTask = executorService.submit(() -> {
            try {
                diskStorage.saveNextSSTable(path, storageBuffer.values(), arena);
                storageBuffer = null;
            } catch (IOException e) {
                throw new IllegalStateException("Can not flush", e);
            }
        });
    }

    @Override
    public void close() throws IOException {
        try {
            if (compactionTask != null && !compactionTask.isDone() && !compactionTask.isCancelled()) {
                compactionTask.get();
            }
            if (flushTask != null && !flushTask.isDone()) {
                flushTask.get();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (ExecutionException e) {
            throw new IllegalStateException("Dao can not be stopped gracefully", e);
        }
        executorService.close();

        if (!storage.isEmpty()) {
            diskStorage.saveNextSSTable(path, storage.values(), arena);
        }

        if (!arena.scope().isAlive()) {
            return;
        }
        arena.close();
    }
}
