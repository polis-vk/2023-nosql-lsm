package ru.vk.itmo.prokopyevnikita;

import ru.vk.itmo.Config;
import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class DaoImpl implements Dao<MemorySegment, Entry<MemorySegment>> {

    private final Config config;
    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    private final ExecutorService executorService = Executors.newSingleThreadExecutor(r -> {
        return new Thread(r, "flusher");
    });
    private volatile State state;

    public DaoImpl(Config config) throws IOException {
        this.config = config;
        this.state = State.initState(config, Storage.load(config));
    }

    @Override
    public Iterator<Entry<MemorySegment>> get(MemorySegment from, MemorySegment to) {
        return combinedIterator(Objects.requireNonNullElse(from, MemorySegment.NULL), to, true);
    }

    public Iterator<Entry<MemorySegment>> getOnlyFromDisk(MemorySegment from, MemorySegment to) {
        return combinedIterator(Objects.requireNonNullElse(from, MemorySegment.NULL), to, false);
    }

    private Iterator<Entry<MemorySegment>> combinedIterator(
            MemorySegment from,
            MemorySegment to,
            boolean includeMemoryAndFlushing
    ) {
        State currenState = accessStateAndCloseCheck();
        if (includeMemoryAndFlushing) {
            Iterator<Entry<MemorySegment>> memoryIterator = currenState.memory.get(from, to);
            Iterator<Entry<MemorySegment>> flushingIterator = currenState.flushing.get(from, to);
            return currenState.storage.getIterator(from, to, memoryIterator, flushingIterator);
        }
        return currenState.storage.getIterator(from, to, null, null);
    }

    @Override
    public Entry<MemorySegment> get(MemorySegment key) {
        Iterator<Entry<MemorySegment>> iterator = get(key, null);
        if (!iterator.hasNext()) {
            return null;
        }
        Entry<MemorySegment> next = iterator.next();
        if (MemorySegmentComparator.compare(key, next.key()) == 0) {
            return next;
        }
        return null;

    }

    @Override
    public void upsert(Entry<MemorySegment> entry) {
        State currenState = accessStateAndCloseCheck();
        boolean flush = currenState.memory.put(entry.key(), entry);
        if (flush) {
            flushInBackground(false);
        }
    }

    private void flushInBackground(boolean canBeParallel) {

        executorService.execute(() -> {
            lock.writeLock().lock();
            try {
                State currenState = accessStateAndCloseCheck();
                if (currenState.isFlushing()) {
                    if (canBeParallel) {
                        return;
                    }
                    throw new AlreadyFlushingInBg();
                }
                currenState = currenState.prepareForFlush();
                this.state = currenState;
            } finally {
                lock.writeLock().unlock();
            }
            try {
                State currenState = accessStateAndCloseCheck();

                Storage.save(config, currenState.flushing.values());
                Storage newStorage = Storage.load(config);

                lock.writeLock().lock();
                try {
                    this.state = currenState.afterFlush(newStorage);
                } finally {
                    lock.writeLock().unlock();
                }
            } catch (IOException e) {
                throw new IllegalStateException(e);
            }
        });

    }

    @Override
    public void flush() throws IOException {
        boolean flush = false;
        lock.writeLock().lock();
        try {
            flush = state.memory.overflow();
        } finally {
            lock.writeLock().unlock();
        }

        if (flush) {
            flushInBackground(true);
        }
    }

    // only single thread can call this method
    @Override
    public synchronized void close() throws IOException {
        State currenState = this.state;
        if (currenState.closed) {
            return;
        }
        executorService.shutdown();
        // await for all tasks to complete
        // it can take a lot of time depending on the size of the database
        try {
            while (!executorService.awaitTermination(12, TimeUnit.HOURS)) {
                wait(10);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        currenState.storage.close();
        this.state = currenState.afterClose();
        if (currenState.memory.isEmpty()) {
            return;
        }
        Storage.save(config, currenState.memory.values());
    }

    @Override
    public void compact() {
        State currenState = accessStateAndCloseCheck();

        if (currenState.memory.isEmpty() && currenState.storage.isCompacted()) {
            return;
        }

        executorService.execute(() -> {
            State stateCompaction = accessStateAndCloseCheck();

            if (stateCompaction.memory.isEmpty() && stateCompaction.storage.isCompacted()) {
                return;
            }

            // compact only ssTables
            Storage storage = null;
            try {
                Storage.compact(config,
                        () -> new MergeSkipNullValuesIterator(
                                List.of(
                                        new OrderedPeekIteratorImpl(0,
                                                getOnlyFromDisk(null, null)
                                        )
                                )
                        )
                );
                storage = Storage.load(config);
            } catch (IOException e) {
                throw new IllegalStateException(e);
            }

            lock.writeLock().lock();
            try {
                this.state = stateCompaction.afterCompaction(storage);
            } finally {
                lock.writeLock().unlock();
            }
        });
    }

    private State accessStateAndCloseCheck() {
        State currenState = this.state;
        if (currenState.closed) {
            throw new IllegalStateException("DAO is Already closed");
        }
        return currenState;
    }
}
