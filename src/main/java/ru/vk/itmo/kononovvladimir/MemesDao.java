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
    private final ReadWriteLock stateLock = new ReentrantReadWriteLock();
    private final long flushThresholdBytes;
    private State state;

    private final ReadWriteLock memoryLock = new ReentrantReadWriteLock();

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
        stateLock.readLock().lock();
        try {
            tmpState = this.state;
        } finally {
            stateLock.readLock().unlock();
        }

        return tmpState;
    }

    private State getStateUnderWriteLock() {
        State tmpState;
        stateLock.writeLock().lock();
        try {
            tmpState = this.state;
        } finally {
            stateLock.writeLock().unlock();
        }

        return tmpState;
    }


    @Override
    public Iterator<Entry<MemorySegment>> get(MemorySegment from, MemorySegment to) {
        State tmpState = getStateUnderReadLock();

        Iterator<Entry<MemorySegment>> memoryIterator = getInMemory(tmpState.memoryStorage, from, to);
        List<Iterator<Entry<MemorySegment>>> merged = new ArrayList<>();
        merged.add(memoryIterator);
        if (!(flushTask == null || flushTask.isDone())) {
            Iterator<Entry<MemorySegment>> flushIterator = getInMemory(tmpState.flushingMemoryTable, from, to);
            merged.addFirst(flushIterator);
        }
        return tmpState.diskStorage.range(merged, from, to);
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

    private boolean taskIsWorking(Future<?> task) {
        return task != null && !task.isDone();
    }

    @Override
    public void upsert(Entry<MemorySegment> entry) {
        State tmpState = getStateUnderWriteLock();

        //long entrySize = calculateSize(entry);
        memoryLock.readLock().lock();
        try {
            long valueSize;
            if (entry.value() == null) {
                valueSize = Long.BYTES;
            } else {
                valueSize = entry.value().byteSize();
            }
            Entry<MemorySegment> prev = tmpState.memoryStorage.put(entry.key(), entry);
            if (prev == null) {
                tmpState.memoryStorageSizeInBytes.addAndGet(entry.key().byteSize() + valueSize);
            } else {
                tmpState.memoryStorageSizeInBytes.addAndGet(valueSize);
            }
        } finally {
            memoryLock.readLock().unlock();
        }
        if (flushThresholdBytes < tmpState.memoryStorageSizeInBytes.get()) {
            // if not flushing throw
            try {
                autoFlush();
            } catch (IOException e) {
                throw new IllegalStateException("Flush не удался", e);
            }
        }
    }

    @Override
    public Entry<MemorySegment> get(MemorySegment key) {
        State tmpState = getStateUnderReadLock();
        Entry<MemorySegment> entry = tmpState.memoryStorage.get(key);
        if (entry == null && tmpState.flushingMemoryTable != null) {
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


    @Override
    public synchronized void compact() throws IOException {
        if (taskIsWorking(taskCompact)) {
            return;
        }
        taskCompact = executorService.submit(() -> {
            try {
                DiskStorage.compact(path, this::all);
            } catch (IOException e) {
                throw new RuntimeException("Compact не удался", e);
            }
        });
    }

    @Override
    public synchronized void flush() throws IOException {
        autoFlush();
    }

    private synchronized void autoFlush() throws IOException {
        State tmpState = getStateUnderWriteLock();
        if (isClosed.get() || taskIsWorking(flushTask)) {
            return;
        }
        stateLock.writeLock().lock();
        try {
            this.state = new State(
                    tmpState.memoryStorage,
                    new ConcurrentSkipListMap<>(comparator),
                    0,
                    tmpState.diskStorage);
        } finally {
            stateLock.writeLock().unlock();
        }
        State tmpState1 = getStateUnderReadLock();
        flushTask = executorService.submit(() -> {

            if (!tmpState1.flushingMemoryTable.isEmpty()) {
                stateLock.writeLock().lock();
                try {
                    tmpState1.diskStorage.saveNextSSTable(path, tmpState1.flushingMemoryTable.values(), arena);
                    this.state = new State(
                            tmpState1.memoryStorage,
                            new ConcurrentSkipListMap<>(comparator),
                            tmpState1.diskStorage
                    );
                } catch (IOException e) {
                    throw new IllegalStateException("Flush не удался", e);
                }
                stateLock.writeLock().unlock();
            }
        });


    }

    @Override
    public void close() throws IOException {
        try {
            if (taskIsWorking(taskCompact)) {
                taskCompact.get();
            }
            if (taskIsWorking(flushTask)) {
                flushTask.get();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return;
        } catch (ExecutionException e) {
            throw new IllegalStateException("Не получилось завершить Dao", e);
        }
        executorService.close();
        State tmpState = getStateUnderWriteLock();
        if (!tmpState.memoryStorage.isEmpty()) {
            tmpState.diskStorage.saveNextSSTable(path, tmpState.memoryStorage.values(), arena);
        }
        if (arena.scope().isAlive()) {
            arena.close();
        }
    }
}



/*    private Long calculateSize(Entry<MemorySegment> entry) {
        return Long.BYTES + entry.key().byteSize() + Long.BYTES
                + (entry.value() == null ? 0 : entry.key().byteSize());
    }*/