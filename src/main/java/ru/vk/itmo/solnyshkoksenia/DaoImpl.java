package ru.vk.itmo.solnyshkoksenia;

import ru.vk.itmo.Config;
import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;
import ru.vk.itmo.solnyshkoksenia.storage.DiskStorage;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class DaoImpl implements Dao<MemorySegment, Entry<MemorySegment>> {
    private static final Comparator<MemorySegment> comparator = new MemorySegmentComparator();
    private final ExecutorService executor = Executors.newSingleThreadExecutor();
    private final ReadWriteLock lock = new ReentrantReadWriteLock(true);
    private final Arena arena;
    private final Path path;
    private volatile State curState;

    public DaoImpl(Config config) throws IOException {
        path = config.basePath().resolve("data");
        Files.createDirectories(path);

        arena = Arena.ofShared();

        this.curState = new State(config, new DiskStorage(DiskStorage.loadOrRecover(path, arena), path));
    }

    @Override
    public Iterator<Entry<MemorySegment>> get(MemorySegment from, MemorySegment to) { // todo
        State state = this.curState.checkAndGet();
        List<Iterator<Triple<MemorySegment>>> iterators = List.of(
                state.getInMemory(state.flushingStorage, from, to),
                state.getInMemory(state.storage, from, to)
        );

        Iterator<Triple<MemorySegment>> iterator = new MergeIterator<>(iterators,
                (e1, e2) -> comparator.compare(e1.key(), e2.key()));

        return new FilterIterator(state.diskStorage.range(iterator, from, to));
    }

    public void upsert(Entry<MemorySegment> entry, Long ttl) {
        State state = this.curState.checkAndGet();

        lock.readLock().lock();
        try {
            state.putInMemory(entry, ttl);
        } finally {
            lock.readLock().unlock();
        }

        if (state.isOverflowed()) {
            try {
                autoFlush();
            } catch (IOException e) {
                throw new RuntimeException("Memory storage overflowed. Cannot flush: " + e.getMessage());
            }
        }
    }

    @Override
    public void upsert(Entry<MemorySegment> entry) {
        upsert(entry, null);
    }

    @Override
    public Entry<MemorySegment> get(MemorySegment key) { // todo
        State state = this.curState.checkAndGet();
        return state.get(key, comparator);
    }

    @Override
    public void flush() throws IOException {
        State state = this.curState.checkAndGet();
        if (state.storage.isEmpty() || state.isFlushing()) {
            return;
        }
        autoFlush();
    }

    private void autoFlush() throws IOException {
        State state = this.curState.checkAndGet();
        lock.writeLock().lock();
        try {
            if (state.isFlushing()) {
                if (state.isOverflowed()) {
                    throw new IOException();
                } else {
                    return;
                }
            }
            this.curState = state.moveStorage();
        } finally {
            lock.writeLock().unlock();
        }

        executor.execute(this::tryFlush);
    }

    private void tryFlush() {
        State state = this.curState.checkAndGet();
        try {
            state.flush();

            lock.writeLock().lock();
            try {
                this.curState = new State(state.config, state.storage, new ConcurrentSkipListMap<>(comparator),
                        new DiskStorage(DiskStorage.loadOrRecover(path, arena), path));
            } catch (IOException e) {
                throw new RuntimeException("Cannot recover storage on disk");
            } finally {
                lock.writeLock().unlock();
            }
        } catch (IOException e) {
            throw new RuntimeException("Flush failed: " + e.getMessage());
        }
    }

    @Override
    public void compact() throws IOException {
        try {
            executor.submit(this::tryCompact).get();
        } catch (InterruptedException e) {
            throw new RuntimeException("Compaction failed. Thread interrupted: " + e.getMessage());
        } catch (ExecutionException e) {
            throw new RuntimeException("Compaction failed: " + e.getMessage());
        }
    }

    private Object tryCompact() {
        State state = this.curState.checkAndGet();
        try {
            state.diskStorage.compact();
        } catch (IOException e) {
            throw new RuntimeException("Cannot compact: " + e.getMessage());
        }

        lock.writeLock().lock();
        try {
            this.curState = new State(state.config, state.storage, state.flushingStorage,
                    new DiskStorage(DiskStorage.loadOrRecover(path, arena), path));
        } catch (IOException e) {
            throw new RuntimeException("Cannot recover storage on disk after compaction: " + e.getMessage());
        } finally {
            lock.writeLock().unlock();
        }

        return null;
    }

    @Override
    public synchronized void close() throws IOException {
        State state = this.curState;
        if (state.isClosed() || !arena.scope().isAlive()) {
            return;
        }

        if (!state.storage.isEmpty()) {
            state.save();
        }

        executor.close();
        arena.close();

        this.curState = state.close();
    }
}
