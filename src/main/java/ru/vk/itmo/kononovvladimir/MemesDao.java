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
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class MemesDao implements Dao<MemorySegment, Entry<MemorySegment>> {

    private static class State {

        private final NavigableMap<MemorySegment, Entry<MemorySegment>> memoryStorage;
        private final NavigableMap<MemorySegment, Entry<MemorySegment>> flushingMemoryTable;
        private final AtomicLong memoryStorageSizeInBytes = new AtomicLong();
        private final DiskStorage diskStorage;

        private State(NavigableMap<MemorySegment, Entry<MemorySegment>> memoryStorage,
                      NavigableMap<MemorySegment, Entry<MemorySegment>> flushingMemoryTable,
                      DiskStorage diskStorage) {
            this.memoryStorage = memoryStorage;
            this.flushingMemoryTable = flushingMemoryTable;
            this.memoryStorageSizeInBytes.getAndSet(memoryStorage.size());
            this.diskStorage = diskStorage;
        }
    }

    private final Comparator<MemorySegment> comparator = MemesDao::compare;
    private Arena arena;
    private final Path path;
    private final AtomicBoolean isClosed = new AtomicBoolean(false);
    private final ReadWriteLock memoryLock = new ReentrantReadWriteLock();
    private final Lock lock = new ReentrantLock();
    private final long flushThresholdBytes;
    private State state;

    private final ExecutorService executorService = Executors.newSingleThreadExecutor();

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

    @Override
    public Iterator<Entry<MemorySegment>> get(MemorySegment from, MemorySegment to) {
        Iterator<Entry<MemorySegment>> memoryIterator = getInMemory(state.memoryStorage, from, to);
        Iterator<Entry<MemorySegment>> flushIterator = getInMemory(state.flushingMemoryTable, from, to);
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
        if (isClosed.get()) {
            //throw
            return;
        }
        long entrySize = calculateSize(entry);
        if (flushThresholdBytes < state.memoryStorageSizeInBytes.get() + entrySize) {
            // if not flushing throw
            if (!state.flushingMemoryTable.isEmpty()) {
                //throw
                return;
            }

            memoryLock.writeLock().lock();
            try {
                this.state = new State(new ConcurrentSkipListMap<>(comparator), state.memoryStorage, state.diskStorage);
                state.memoryStorage.put(entry.key(), entry);
            } finally {
                memoryLock.writeLock().unlock();
            }
            executorService.execute(this::autoFlush);
            return;
        }

        memoryLock.writeLock().lock();
        try {
            state.memoryStorage.put(entry.key(), entry);
            state.memoryStorageSizeInBytes.addAndGet(entrySize);
        } finally {
            memoryLock.writeLock().unlock();
        }
    }

    @Override
    public Entry<MemorySegment> get(MemorySegment key) {
        Entry<MemorySegment> entry = state.memoryStorage.get(key);
        if (entry == null) {
            entry = state.flushingMemoryTable.get(key);
        }
        if (entry != null) {
            if (entry.value() == null) {
                return null;
            }
            return entry;
        }

        Iterator<Entry<MemorySegment>> iterator = state.diskStorage.range(List.of(Collections.emptyIterator()), key, null);

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
    public synchronized void compact() throws IOException {
        if (isClosed.get()) {
            //throw
            return;
        }

        executorService.execute(() -> {
            try {
                DiskStorage.compact(path, this::all);
            } catch (IOException e) {
                throw new RuntimeException("Error during compaction", e);
            }
        });
    }

    @Override
    public void flush() throws IOException {
        memoryLock.writeLock().lock();
        try {
            lock.lock();
            try {
                if (!state.memoryStorage.isEmpty()) {
                    DiskStorage.saveNextSSTable(path, state.memoryStorage.values());
                }
            } finally {
                lock.unlock();
            }
        } finally {
            memoryLock.writeLock().unlock();
        }
        state = new State(
                new ConcurrentSkipListMap<>(comparator),
                state.flushingMemoryTable,
                new DiskStorage(DiskStorage.loadOrRecover(path, arena))
        );
    }

    private void autoFlush() {
        DiskStorage tmpStorage;
        try {
            arena.close();
            this.arena = Arena.ofShared();
            memoryLock.writeLock().lock();
            try {
                lock.lock();
                try {
                    if (!state.memoryStorage.isEmpty()) {
                        DiskStorage.saveNextSSTable(path, state.flushingMemoryTable.values());
                    }
                } finally {
                    lock.unlock();
                }
            } finally {
                memoryLock.writeLock().unlock();
            }
            tmpStorage = new DiskStorage(DiskStorage.loadOrRecover(path, arena));
        } catch (IOException e) {
            throw new RuntimeException("Error during autoFlush", e);
        }

        memoryLock.writeLock().lock();
        try {
            this.state = new State(state.memoryStorage, new ConcurrentSkipListMap<>(comparator), tmpStorage);
        } finally {
            memoryLock.writeLock().unlock();
        }
    }

    @Override
    public synchronized void close() throws IOException {
        if (!arena.scope().isAlive()) {
            return;
        }
        memoryLock.writeLock().lock();
        try {
            lock.lock();
            try {
                arena.close();

                if (!state.memoryStorage.isEmpty()) {
                    DiskStorage.saveNextSSTable(path, state.memoryStorage.values());
                }
            } finally {
                lock.unlock();
            }
        } finally {
            memoryLock.writeLock().unlock();
        }
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