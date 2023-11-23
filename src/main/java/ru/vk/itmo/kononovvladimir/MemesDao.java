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
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class MemesDao implements Dao<MemorySegment, Entry<MemorySegment>> {

    private final Comparator<MemorySegment> comparator = MemesDao::compare;
    private final Arena arena;
    private final Path path;

    NavigableMap<MemorySegment, Entry<MemorySegment>> memoryStorage;
    NavigableMap<MemorySegment, Entry<MemorySegment>> flushingMemoryTable;
    final AtomicLong memoryStorageSizeInBytes = new AtomicLong();
    final DiskStorage diskStorage;
    private final AtomicBoolean isClosed = new AtomicBoolean(false);
    //private final ReadWriteLock memoryLock = new ReentrantReadWriteLock();
    private final long flushThresholdBytes;
    //private State state;

    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    private final ExecutorService executorService = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());

    private Future<?> taskCompact;

    private Future<?> flushTask;


    public MemesDao(Config config) throws IOException {
        this.path = config.basePath().resolve("data");
        Files.createDirectories(path);

        this.flushThresholdBytes = config.flushThresholdBytes();
        this.arena = Arena.ofShared();
        this.memoryStorage = new ConcurrentSkipListMap<>(comparator);
        this.flushingMemoryTable = new ConcurrentSkipListMap<>(comparator);
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

        //State tmpState = getStateUnderReadLock();

        Iterator<Entry<MemorySegment>> memoryIterator = getInMemory(memoryStorage, from, to);
        List<Iterator<Entry<MemorySegment>>> merged = new ArrayList<>();
        merged.add(memoryIterator);
        if (!(flushTask == null || flushTask.isDone())) {
            Iterator<Entry<MemorySegment>> flushIterator = getInMemory(flushingMemoryTable, from, to);
            merged.addFirst(flushIterator);
        }
        return diskStorage.range(merged, from, to);

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
        if (isClosed.get() || (!(flushTask == null || flushTask.isDone()) && memoryStorageSizeInBytes.get() >= flushThresholdBytes)) {
            throw new IllegalStateException("Previous flush has not yet ended");
        }
        //State tmpState = getStateUnderWriteLock();

        //long entrySize = calculateSize(entry);
        lock.readLock().lock();
        try {
            long valueSize;
            if (entry.value() == null) {
                valueSize = Long.BYTES;
            } else {
                valueSize = entry.value().byteSize();
            }
            Entry<MemorySegment> prev = memoryStorage.put(entry.key(), entry);
            if (prev == null) {
                memoryStorageSizeInBytes.addAndGet(entry.key().byteSize() + valueSize);
            } else {
                memoryStorageSizeInBytes.addAndGet(valueSize);
            }
        } finally {
            lock.readLock().unlock();
        }
        if (flushThresholdBytes < memoryStorageSizeInBytes.get()) {
            // if not flushing throw
            try {
                autoFlush();
            } catch (IOException e) {
                throw new IllegalStateException("Can not flush", e);
            }
        }
    }

    @Override
    public Entry<MemorySegment> get(MemorySegment key) {
        //State tmpState = getStateUnderReadLock();
        Entry<MemorySegment> entry = memoryStorage.get(key);
        if (entry == null && flushingMemoryTable != null) {
            entry = flushingMemoryTable.get(key);
        }
        if (entry != null) {
            if (entry.value() == null) {
                return null;
            }
            return entry;
        }

        Iterator<Entry<MemorySegment>> iterator = diskStorage.range(List.of(Collections.emptyIterator()), key, null);

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
    public synchronized void flush() throws IOException {
        autoFlush();
    }

    private synchronized void autoFlush() throws IOException {
        //  State tmpState = getStateUnderWriteLock();

        if (!(flushTask == null || flushTask.isDone()) || memoryStorage.isEmpty()) {
            return;
        }
        lock.writeLock().lock();
        try {
            flushingMemoryTable = memoryStorage;
            memoryStorage = new ConcurrentSkipListMap<>(comparator);
            memoryStorageSizeInBytes.set(0);
            //this.state = new State(new ConcurrentSkipListMap<>(comparator), state.memoryStorage, 0, state.diskStorage);
        } finally {
            lock.writeLock().unlock();
        }
        flushTask = executorService.submit(() -> {

            if (!flushingMemoryTable.isEmpty()) {
                try {
                    diskStorage.saveNextSSTable(path, flushingMemoryTable.values(), arena);
                    flushingMemoryTable = new ConcurrentSkipListMap<>(comparator);
                } catch (IOException e) {
                    throw new IllegalStateException("Can not flush", e);
                }
            }
        });


    }

    @Override
    public void close() throws IOException {
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

        if (!memoryStorage.isEmpty()) {
            diskStorage.saveNextSSTable(path, memoryStorage.values(), arena);
        }
        if (!arena.scope().isAlive()) {
            return;
        }
        arena.close();
    }
}



/*    private Long calculateSize(Entry<MemorySegment> entry) {
        return Long.BYTES + entry.key().byteSize() + Long.BYTES
                + (entry.value() == null ? 0 : entry.key().byteSize());
    }*/

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