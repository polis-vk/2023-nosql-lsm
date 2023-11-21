package ru.vk.itmo.volkovnikita;

import ru.vk.itmo.Config;
import ru.vk.itmo.Dao;
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

public class DaoImpl implements Dao<MemorySegment, Entry<MemorySegment>> {

    private final Comparator<MemorySegment> comparator = DaoImpl::compare;
    private NavigableMap<MemorySegment, Entry<MemorySegment>> storage =
            new ConcurrentSkipListMap<>(comparator);
    private final Arena arena;
    private final DiskStorage diskStorage;
    private final Path path;
    private final long flushThresholdBytes;
    private final ExecutorService executorService = Executors.newFixedThreadPool(5);
    private final AtomicLong totalSizeOfStorage = new AtomicLong(0);
    private NavigableMap<MemorySegment, Entry<MemorySegment>> temporaryStorage =
            new ConcurrentSkipListMap<>(comparator);

    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    private Future<?> ongoingFlushOperation;
    private Future<?> ongoingCompactionOperation;

    public DaoImpl(Config config) throws IOException {
        this.path = config.basePath().resolve("data");
        this.flushThresholdBytes = config.flushThresholdBytes();
        Files.createDirectories(path);

        arena = Arena.ofShared();

        this.diskStorage = new DiskStorage(DiskStorage.loadOrRecover(path, arena));
    }

    @Override
    public Iterator<Entry<MemorySegment>> get(MemorySegment from, MemorySegment to) {
        List<Iterator<Entry<MemorySegment>>> inMem = new ArrayList<>(List.of(cutInMemory(storage, from, to)));
        if (!(ongoingFlushOperation == null || ongoingFlushOperation.isDone())) {
            inMem.addFirst(cutInMemory(temporaryStorage, from, to));
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
        if (isFlushOperationPending() && isFlushThresholdExceeded()) {
            throw new IllegalStateException("Awaiting completion of the ongoing flush operation");
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
                totalSizeOfStorage.addAndGet(entry.key().byteSize() + valueSize);
            } else {
                totalSizeOfStorage.addAndGet(valueSize);
            }
        } finally {
            lock.readLock().unlock();
        }

        if (totalSizeOfStorage.get() >= flushThresholdBytes) {
            try {
                flush();
            } catch (IOException e) {
                throw new IllegalStateException("Failed during flushing process", e);
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

        if (temporaryStorage != null) {
            Entry<MemorySegment> bufferEntry = temporaryStorage.get(key);
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
        if (isCompactionOperationPending()) {
            return;
        }

        ongoingCompactionOperation = executorService.submit(() -> {
            try {
                DiskStorage.compact(path, this::all);
            } catch (IOException e) {
                throw new IllegalStateException("Compaction process failed", e);
            }
        });
    }

    @Override
    public synchronized void flush() throws IOException {
        if (isFlushOperationPending() || storage.isEmpty()) {
            return;
        }

        lock.writeLock().lock();
        try {
            temporaryStorage = storage;
            storage = new ConcurrentSkipListMap<>(comparator);
            totalSizeOfStorage.set(0);
        } finally {
            lock.writeLock().unlock();
        }

        ongoingFlushOperation = executorService.submit(() -> {
            try {
                diskStorage.save(path, temporaryStorage.values(), arena);
                temporaryStorage = null;
            } catch (IOException e) {
                throw new IllegalStateException("Flush operation unsuccessful", e);
            }
        });
    }

    @Override
    public void close() throws IOException {
        try {
            if (isCompactionOperationPending() && !ongoingCompactionOperation.isCancelled()) {
                ongoingCompactionOperation.get();
            }
            if (isFlushOperationPending()) {
                ongoingFlushOperation.get();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (ExecutionException e) {
            throw new IllegalStateException("Graceful termination of Dao is not possible", e);
        }
        executorService.close();

        if (!storage.isEmpty()) {
            diskStorage.save(path, storage.values(), arena);
        }

        if (!arena.scope().isAlive()) {
            return;
        }
        arena.close();
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

    private boolean isFlushOperationPending() {
        return ongoingFlushOperation != null && !ongoingFlushOperation.isDone();
    }

    private boolean isCompactionOperationPending() {
        return ongoingCompactionOperation != null && !ongoingCompactionOperation.isDone();
    }

    private boolean isFlushThresholdExceeded() {
        return totalSizeOfStorage.get() >= flushThresholdBytes;
    }
}
