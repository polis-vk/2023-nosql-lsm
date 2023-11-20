package ru.vk.itmo.tyapuevdmitrij;

import ru.vk.itmo.Config;
import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class MemorySegmentDao implements Dao<MemorySegment, Entry<MemorySegment>> {
    private final Path ssTablePath;
    private final Config config;
    private State state;
    private final Lock storageLock = new ReentrantLock();
    private final ExecutorService executor =
            Executors.newSingleThreadExecutor(task -> new Thread(task, "DaoMemorySegment"));
    private final ReadWriteLock upsertLock = new ReentrantReadWriteLock();
    private final AtomicBoolean autoFlushing = new AtomicBoolean();

    public MemorySegmentDao() {
        ssTablePath = null;
        config = null;
    }

    public MemorySegmentDao(Config config) {
        ssTablePath = config.basePath();
        state = new State(new ConcurrentSkipListMap<>(MemorySegmentComparator.getMemorySegmentComparator()),
                null,
                new Storage(ssTablePath));
        this.config = config;
    }

    @Override
    public Iterator<Entry<MemorySegment>> get(MemorySegment from, MemorySegment to) {
        State currentState = this.state;
        List<Iterator<Entry<MemorySegment>>> iterators = new ArrayList<>();
        iterators.add(getMemTableIterator(from, to, currentState.flushMemTable));
        iterators.add(getMemTableIterator(from, to, currentState.memTable));
        if (currentState.storage.ssTablesQuantity == 0) {
            return new MergeIterator(iterators, Comparator.comparing(Entry::key,
                    MemorySegmentComparator.getMemorySegmentComparator()));
        } else {
            return currentState.storage.range(iterators.get(0),
                    iterators.get(1),
                    from,
                    to,
                    MemorySegmentComparator.getMemorySegmentComparator());
        }
    }

    private Iterator<Entry<MemorySegment>> getMemTableIterator(MemorySegment from, MemorySegment to,
                                                               NavigableMap<MemorySegment,
                                                                       Entry<MemorySegment>> memTable) {
        if (memTable == null) {
            return Collections.emptyIterator();
        }
        if (from == null && to == null) {
            return memTable.values().iterator();
        }
        if (from == null) {
            return memTable.headMap(to).values().iterator();
        }
        if (to == null) {
            return memTable.tailMap(from).values().iterator();
        }
        return memTable.subMap(from, to).values().iterator();
    }

    @Override
    public Entry<MemorySegment> get(MemorySegment key) {
        State currentState = this.state;
        Entry<MemorySegment> value = currentState.memTable.get(key);
        if (value != null && value.value() == null) {
            return null;
        }
        if (value == null && currentState.flushMemTable != null) {
            value = currentState.flushMemTable.get(key);
        }
        if (value != null && value.value() == null) {
            return null;
        }
        if (value != null || currentState.storage.ssTables == null) {
            return value;
        }
        Iterator<Entry<MemorySegment>> iterator = currentState.storage.range(Collections.emptyIterator(),
                Collections.emptyIterator(),
                key,
                null,
                MemorySegmentComparator.getMemorySegmentComparator());

        if (!iterator.hasNext()) {
            return null;
        }
        Entry<MemorySegment> next = iterator.next();
        if (MemorySegmentComparator.getMemorySegmentComparator().compare(next.key(), key) == 0) {
            return next;
        }
        return null;
    }

    @Override
    public void upsert(Entry<MemorySegment> entry) {
        State currentState = this.state;
        if (config == null || config.flushThresholdBytes() == 0) {
            currentState.memTable.put(entry.key(), entry);
            return;
        }
        upsertLock.readLock().lock();
        try {
            Entry<MemorySegment> entryBySameKey = currentState.memTable.get(entry.key());
            long entryBySameKeyByteSize = entry.key().byteSize();
            if (entryBySameKey != null) {
                entryBySameKeyByteSize = entryBySameKey.value() == null ? 0 : entryBySameKey.value().byteSize();
            }
            long entrySize = entry.key().byteSize();
            entrySize += entry.value() == null ? 0 : entry.value().byteSize();
            long entrySizeDelta = entrySize - entryBySameKeyByteSize;
            long currentMemoryUsage = currentState.memoryUsage.addAndGet(entrySizeDelta);
            if (currentMemoryUsage > config.flushThresholdBytes()) {
                if (autoFlushing.get() && currentState.flushMemTable != null) {
                 throw new FilesException("dont upsert - overload");
                }
                if (!autoFlushing.getAndSet(true)) {
                    executor.execute(this::flushing);
                }
            }
            this.state.memTable.put(entry.key(), entry);
        } finally {
            upsertLock.readLock().unlock();
        }
    }

    @Override
    public void flush() throws IOException {
        if (state.memTable.isEmpty() || state.flushMemTable != null) {
            return;
        }
        flushing();
    }

    public void flushing() {
        storageLock.lock();
        try {
            upsertLock.writeLock().lock();
            try {
                state = new State(new ConcurrentSkipListMap<>(MemorySegmentComparator.getMemorySegmentComparator()),
                        state.memTable,
                        state.storage);
            } finally {
                upsertLock.writeLock().unlock();
            }
            if (this.state.flushMemTable.isEmpty()) {
                return;
            }
            state.storage.save(state.flushMemTable.values(), ssTablePath, state.storage);
            Storage loadFlushed = new Storage(ssTablePath);
            upsertLock.writeLock().lock();
            try {
                state = new State(state.memTable,
                        null,
                        loadFlushed);
            } finally {
                upsertLock.writeLock().unlock();
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } finally {
            autoFlushing.set(false);
            storageLock.unlock();
        }
    }

    @Override
    public void compact() throws IOException {
        State currentState = this.state;
        if (currentState.storage.ssTablesQuantity <= 1 && currentState.memTable.isEmpty()) {
            return;
        }
        storageLock.lock();
        try {
            executor.execute(() -> {
                State stateNow = this.state;
                storageLock.lock();
                try {
                    if (!currentState.storage.readArena.scope().isAlive()) {
                        return;
                    }
                    stateNow.storage.storageHelper.saveDataForCompaction(stateNow, ssTablePath);
                    stateNow.storage.storageHelper.deleteOldSsTables(ssTablePath);
                    stateNow.storage.storageHelper.renameCompactedSsTable(ssTablePath);
                    Storage load = new Storage(ssTablePath);
                    upsertLock.writeLock().lock();
                    try {
                        this.state = new State(this.state.memTable, this.state.flushMemTable, load);
                    } finally {
                        upsertLock.writeLock().unlock();
                    }
                } finally {
                    storageLock.unlock();
                }
            });
        } finally {
            storageLock.unlock();
        }

    }

    @Override
    public synchronized void close() throws IOException {
        if (state.storage == null) {
            return;
        }
        executor.shutdown();
        try {
            boolean terminated;
            do {
                terminated = executor.awaitTermination(1, TimeUnit.HOURS);
            } while (!terminated);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
        flush();
    }
}
