package ru.vk.itmo.reshetnikovaleksei;

import ru.vk.itmo.Config;
import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;
import ru.vk.itmo.reshetnikovaleksei.iterators.MergeIterator;
import ru.vk.itmo.reshetnikovaleksei.iterators.PeekingIterator;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class DaoImpl implements Dao<MemorySegment, Entry<MemorySegment>> {
    private final ReadWriteLock lock;
    private final Config config;
    private final ExecutorService executor;
    private final AtomicBoolean isClosed;

    private State state;

    public DaoImpl(Config config) throws IOException {
        this.lock = new ReentrantReadWriteLock();
        this.config = config;
        this.executor = Executors.newSingleThreadExecutor();
        this.isClosed = new AtomicBoolean(false);

        this.state = new State(
                new ConcurrentSkipListMap<>(MemorySegmentComparator.getInstance()),
                null,
                new SSTableManager(config)
        );
    }

    @Override
    public Entry<MemorySegment> get(MemorySegment key) {
        Iterator<Entry<MemorySegment>> iterator = allIterators(key, null);

        if (iterator.hasNext()) {
            Entry<MemorySegment> result = iterator.next();
            if (MemorySegmentComparator.getInstance().compare(key, result.key()) == 0) {
                return result.value() == null ? null : result;
            }
        }

        return null;
    }

    @Override
    public Iterator<Entry<MemorySegment>> get(MemorySegment from, MemorySegment to) {
        return allIterators(from, to);
    }

    @Override
    public void upsert(Entry<MemorySegment> entry) {
        if (entry == null || entry.key() == null) {
            throw new IllegalArgumentException("Attempt to upsert empty entry.");
        }

        long valueSize = entry.value() == null ? 0 : entry.value().byteSize();
        long delta = entry.key().byteSize() + valueSize;

        if (state.memoryTableByteSize.get() + delta >= config.flushThresholdBytes()) {
            if (state.flushingMemoryTable != null) {
                throw new IllegalArgumentException("Flushing table is not empty");
            }

            try {
                flush();
            } catch (IOException e) {
                throw new IllegalArgumentException("failed to flush", e);
            }
        }

        lock.readLock().lock();
        try {
            state.memoryTable.put(entry.key(), entry);
            state.memoryTableByteSize.addAndGet((Long.BYTES + entry.key().byteSize() + Long.BYTES + valueSize));
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public synchronized void flush() throws IOException {
        if (state.memoryTable.isEmpty() || state.flushingMemoryTable != null) {
            return;
        }

        lock.writeLock().lock();
        try {
            state = new State(
                    new ConcurrentSkipListMap<>(MemorySegmentComparator.getInstance()),
                    state.memoryTable,
                    state.ssTableManager
            );
        } finally {
            lock.writeLock().unlock();
        }

        executor.execute(this::backgroundFlush);
    }

    @Override
    public void close() throws IOException {
        if (!isClosed.compareAndSet(false, true)) {
            return;
        }

        executor.shutdown();
        try {
            if (!executor.awaitTermination(1, TimeUnit.HOURS)) {
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        if (!state.memoryTable.isEmpty()) {
            state = new State(
                    new ConcurrentSkipListMap<>(MemorySegmentComparator.getInstance()),
                    state.memoryTable,
                    state.ssTableManager
            );
            backgroundFlush();
        }
        state.ssTableManager.close();
        state = null;
    }

    @Override
    public void compact() throws IOException {
        executor.execute(() -> {
            try {
                state.ssTableManager.compact();
            } catch (IOException e) {
                throw new IllegalStateException(e);
            }
        });
    }

    private void backgroundFlush() {
        try {
            state.ssTableManager.flush(state.flushingMemoryTable.values());
            lock.writeLock().lock();
            try {
                state = new State(state.memoryTable, null, state.ssTableManager);
            } finally {
                lock.writeLock().unlock();
            }
        } catch (IOException e) {
            throw new IllegalStateException("Can't flush table", e);
        }
    }

    private Iterator<Entry<MemorySegment>> allIterators(MemorySegment from, MemorySegment to) {
        State currentState = state;

        List<PeekingIterator> iterators = new ArrayList<>();

        iterators.add(new PeekingIterator(memoryIterator(from, to, currentState.memoryTable), 2));
        if (currentState.flushingMemoryTable != null) {
            iterators.add(new PeekingIterator(memoryIterator(from, to, currentState.flushingMemoryTable), 1));
        }
        iterators.add(new PeekingIterator(currentState.ssTableManager.get(from, to), 0));

        return new PeekingIterator(
                MergeIterator.merge(
                        iterators,
                        MemorySegmentComparator.getInstance()
                )
        );
    }

    private Iterator<Entry<MemorySegment>> memoryIterator(
            MemorySegment from, MemorySegment to,NavigableMap<MemorySegment, Entry<MemorySegment>> memoryTable
    ) {
        if (from == null && to == null) {
            return memoryTable.values().iterator();
        }

        if (from == null) {
            return memoryTable.headMap(to).values().iterator();
        }
        if (to == null) {
            return memoryTable.tailMap(from).values().iterator();
        }

        return memoryTable.subMap(from, to).values().iterator();
    }

    private static class State {
        private final NavigableMap<MemorySegment, Entry<MemorySegment>> memoryTable;
        private final NavigableMap<MemorySegment, Entry<MemorySegment>> flushingMemoryTable;
        private final AtomicLong memoryTableByteSize;
        private final SSTableManager ssTableManager;

        private State(
                NavigableMap<MemorySegment, Entry<MemorySegment>> memoryTable,
                NavigableMap<MemorySegment, Entry<MemorySegment>> flushingMemoryTable,
                SSTableManager ssTableManager
        ) {
            this.memoryTable = memoryTable;
            this.flushingMemoryTable = flushingMemoryTable;
            this.ssTableManager = ssTableManager;
            this.memoryTableByteSize = new AtomicLong();
        }
    }
}
