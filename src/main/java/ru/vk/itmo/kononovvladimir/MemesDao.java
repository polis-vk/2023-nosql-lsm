package ru.vk.itmo.kononovvladimir;

import ru.vk.itmo.Config;
import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class MemesDao implements Dao<MemorySegment, Entry<MemorySegment>> {

    private final Comparator<MemorySegment> comparator = MemesDao::compare;
    private final Arena arena;
    private final Path path;
    private final AtomicBoolean isClosed = new AtomicBoolean(false);
    private final ReadWriteLock memoryLock = new ReentrantReadWriteLock();
    private final long flushThresholdBytes;
    private volatile State state;
    private final ExecutorService executorService = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());

    private Future<?> taskCompact;

    private Future<?> flushTask;


    public MemesDao(Config config) throws IOException {
        this.path = config.basePath().resolve("data");
        Files.createDirectories(path);

        this.flushThresholdBytes = config.flushThresholdBytes();
        this.arena = Arena.ofShared();
        this.state = new State(
                new ConcurrentSkipListMap<>(comparator),
                new ConcurrentSkipListMap<>(comparator),
                new DiskStorage(DiskStorage.loadOrRecover(path, arena))
        );
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

    private State getStateUnderReadLock() {
        State tmpState;
        memoryLock.readLock().lock();
        try {
            tmpState = this.state;
        } finally {
            memoryLock.readLock().unlock();
        }

        return tmpState;
    }

    private State getStateUnderWriteLock() {
        State tmpState;
        memoryLock.writeLock().lock();
        try {
            tmpState = this.state;
        } finally {
            memoryLock.writeLock().unlock();
        }

        return tmpState;
    }

    @Override
    public Iterator<Entry<MemorySegment>> get(MemorySegment from, MemorySegment to) {
        if (isClosed.get()) {
            throw new OutOfMemoryError("f");
        }

        State tmpState = getStateUnderReadLock();

        Iterator<Entry<MemorySegment>> memoryIterator = getInMemory(tmpState.memoryStorage, from, to);
        Iterator<Entry<MemorySegment>> flushIterator;
        if (!(flushTask == null || flushTask.isDone())) {
            flushIterator = getInMemory(tmpState.flushingMemoryTable, from, to);
        } else flushIterator = Collections.emptyIterator();
        return state.diskStorage.range(List.of(memoryIterator, flushIterator), from, to);
    }

    private Iterator<Entry<MemorySegment>> getInMemory(
            NavigableMap<MemorySegment, Entry<MemorySegment>> storage,
            MemorySegment from,
            MemorySegment to
    ) {
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
    public void upsert(Entry<MemorySegment> entry) {
        if (isClosed.get() || !(flushTask == null || flushTask.isDone()) && state.memoryStorageSizeInBytes.get() >= flushThresholdBytes) {
            throw new IllegalStateException("Previous flush has not yet ended");
        }
        State tmpState = getStateUnderWriteLock();

        long entrySize = calculateSize(entry);
        memoryLock.writeLock().lock();
        try {
            this.state.memoryStorage.put(entry.key(), entry);
            this.state.memoryStorageSizeInBytes.addAndGet(entrySize);
        } finally {
            memoryLock.writeLock().unlock();
        }
        if (flushThresholdBytes < tmpState.memoryStorageSizeInBytes.get() + entrySize) {
            // if not flushing throw
            autoFlush();
        }


    }

    @Override
    public Entry<MemorySegment> get(MemorySegment key) {
        State tmpState = getStateUnderReadLock();
        Entry<MemorySegment> entry = tmpState.memoryStorage.get(key);
        if (entry == null) {
            entry = tmpState.flushingMemoryTable.get(key);
        }
        if (entry != null) {
            if (entry.value() == null) {
                return null;
            }
            return entry;
        }

        Iterator<Entry<MemorySegment>> iterator = tmpState.diskStorage.range(List.of(Collections.emptyIterator()), key, null);

        if (!iterator.hasNext()) {
            return null;
        }
        Entry<MemorySegment> next = iterator.next();
        if (compare(next.key(), key) == 0) {
            return next;
        }
        return null;
    }

    private Long calculateSize(Entry<MemorySegment> entry) {
        return Long.BYTES + entry.key().byteSize() + Long.BYTES
                + (entry.value() == null ? 0 : entry.key().byteSize());
    }

    @Override
    public void compact() throws IOException {
        if (!(taskCompact == null || taskCompact.isDone())) {
            return;
        }
        taskCompact = executorService.submit(() -> {
            try {
                DiskStorage.compact(path, this::all);
            } catch (IOException e) {
                throw new RuntimeException("Error during compaction", e);
            }
        });
    }

    @Override
    public void flush() throws IOException {
        autoFlush();
    }

    private synchronized void autoFlush() {
        State tmpState = getStateUnderWriteLock();

        if (!(flushTask == null || flushTask.isDone()) || !tmpState.memoryStorage.isEmpty()) {
            return;
        }
        memoryLock.writeLock().lock();
        try {
            this.state = new State(new ConcurrentSkipListMap<>(comparator), tmpState.memoryStorage, tmpState.diskStorage);
        } finally {
            memoryLock.writeLock().unlock();
        }
        flushTask = executorService.submit(() -> {

            try {
                if (!state.flushingMemoryTable.isEmpty()) {
                    state.diskStorage.saveNextSSTable(path, state.flushingMemoryTable.values(), arena);
                    memoryLock.writeLock().lock();
                    try {
                        this.state = new State(state.memoryStorage, new ConcurrentSkipListMap<>(comparator), state.diskStorage);
                    } finally {
                        memoryLock.writeLock().unlock();
                    }
                }
            } catch (IOException e) {
                throw new RuntimeException("Error during autoFlush", e);
            }
        });


    }

    @Override
    public synchronized void close() throws IOException {
        try {
            if (taskCompact != null && !taskCompact.isDone() && !taskCompact.isCancelled()) {
                taskCompact.get();
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

        if (!state.memoryStorage.isEmpty()) {
            state.diskStorage.saveNextSSTable(path, state.memoryStorage.values(), arena);
        }
        if (!arena.scope().isAlive()) {
            return;
        }
        arena.close();
    }
}




/*    private void tryToFlush() {
        try {
            state.diskStorage.flush(state.flushingMemoryTable.values());
            lock.writeLock().lock();
            try {
                state = new State(state.memoryStorage, null, state.diskStorage);
            } finally {
                lock.writeLock().unlock();
            }
        } catch (IOException e) {
            throw new ApplicationException("Can't flush memory table", e);
        }
    }*/