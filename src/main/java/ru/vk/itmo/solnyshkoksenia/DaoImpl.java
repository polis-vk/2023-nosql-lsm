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
    private volatile State state;

    public DaoImpl(Config config) throws IOException {
        path = config.basePath().resolve("data");
        Files.createDirectories(path);

        arena = Arena.ofShared();

        this.state = new State(config, new DiskStorage(DiskStorage.loadOrRecover(path, arena), path));
    }

    @Override
    public Iterator<Entry<MemorySegment>> get(MemorySegment from, MemorySegment to) {
        State curState = this.state.checkAndGet();
        List<Iterator<Entry<MemorySegment>>> iterators = List.of(
                curState.getInMemory(curState.flushingStorage, from, to),
                curState.getInMemory(curState.storage, from, to)
        );

        Iterator<Entry<MemorySegment>> iterator = new MergeIterator<>(iterators,
                (e1, e2) -> comparator.compare(e1.key(), e2.key()));
        return curState.diskStorage.range(iterator, from, to);
    }

    @Override
    public void upsert(Entry<MemorySegment> entry) {
        State curState = this.state.checkAndGet();

        lock.readLock().lock();
        try {
            curState.putInMemory(entry);
        } finally {
            lock.readLock().unlock();
        }

        if (curState.isOverflowed()) {
            try {
                autoFlush();
            } catch (IOException e) {
                throw new DaoException("Memory storage overflowed. Cannot flush: " + e.getMessage());
            }
        }
    }

    @Override
    public Entry<MemorySegment> get(MemorySegment key) {
        State curState = this.state.checkAndGet();
        return curState.get(key, comparator);
    }

    @Override
    public void flush() throws IOException {
        State curState = this.state.checkAndGet();
        if (curState.storage.isEmpty() || curState.isFlushing()) {
            return;
        }
        autoFlush();
    }

    private void autoFlush() throws IOException {
        State curState = this.state.checkAndGet();
        lock.writeLock().lock();
        try {
            if (curState.isFlushing()) {
                throw new IOException();
            }
            this.state = curState.moveStorage();
        } finally {
            lock.writeLock().unlock();
        }

        executor.execute(this::tryFlush);
    }

    private void tryFlush() {
        State curState = this.state.checkAndGet();
        try {
            curState.flush();
        } catch (IOException e) {
            throw new DaoException("Flush failed: " + e.getMessage());
        }

        lock.writeLock().lock();
        try {
            this.state = new State(curState.config, curState.storage, new ConcurrentSkipListMap<>(comparator),
                    new DiskStorage(DiskStorage.loadOrRecover(path, arena), path));
        } catch (IOException e) {
            throw new DaoException("Cannot recover storage on disk");
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public void compact() throws IOException {
        try {
            executor.submit(this::tryCompact).get();
        } catch (InterruptedException e) {
            throw new DaoException("Compaction failed. Thread interrupted: " + e.getMessage());
        } catch (ExecutionException e) {
            throw new DaoException("Compaction failed: " + e.getMessage());
        }
    }

    private Object tryCompact() {
        State curState = this.state.checkAndGet();
        try {
            curState.diskStorage.compact();
        } catch (IOException e) {
            throw new DaoException("Cannot compact: " + e.getMessage());
        }

        lock.writeLock().lock();
        try {
            this.state = new State(curState.config, curState.storage, curState.flushingStorage,
                    new DiskStorage(DiskStorage.loadOrRecover(path, arena), path));
        } catch (IOException e) {
            throw new DaoException("Cannot recover storage on disk after compaction: " + e.getMessage());
        } finally {
            lock.writeLock().unlock();
        }

        return null;
    }

    @Override
    public synchronized void close() throws IOException {
        State curState = this.state;
        if (curState.isClosed() || !arena.scope().isAlive()) {
            return;
        }

        if (!curState.storage.isEmpty()) {
            curState.save();
        }

        executor.shutdown();
        executor.close();
        arena.close();

        this.state = curState.close();
    }
}
