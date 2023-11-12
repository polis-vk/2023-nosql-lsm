package ru.vk.itmo.dyagayalexandra;

import ru.vk.itmo.Config;
import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class Storage implements Dao<MemorySegment, Entry<MemorySegment>> {

    private final FileManager fileManager;
    private final AtomicBoolean isClosed;
    private final AtomicBoolean isBackgroundFlushing;
    private final List<Future<?>> taskResults;
    private final ExecutorService service;
    private long flushThresholdBytes;
    private State state;
    private final ReadWriteLock lock;
    private final Object object;

    public Storage() {
        fileManager = null;
        isClosed = new AtomicBoolean(false);
        isBackgroundFlushing = new AtomicBoolean(false);
        taskResults = new CopyOnWriteArrayList<>();
        lock = new ReentrantReadWriteLock();
        object = new Object();
        service = Executors.newSingleThreadExecutor(r -> new Thread(r, "BackgroundFlushAndCompact"));
    }

    public Storage(Config config) {
        fileManager = new FileManager(config);
        state = State.emptyState(fileManager);
        isClosed = new AtomicBoolean(false);
        isBackgroundFlushing = new AtomicBoolean(false);
        taskResults = new CopyOnWriteArrayList<>();
        lock = new ReentrantReadWriteLock();
        object = new Object();
        service = Executors.newSingleThreadExecutor(r -> new Thread(r, "BackgroundFlushAndCompact"));
        flushThresholdBytes = config.flushThresholdBytes();
    }

    @Override
    public Iterator<Entry<MemorySegment>> get(MemorySegment from, MemorySegment to) {
        if (isClosed.get()) {
            throw new RuntimeException("Unable to get: close operation performed.");
        }

        State currentState;
        lock.readLock().lock();
        try {
            currentState = this.state;
        } finally {
            lock.readLock().unlock();
        }

        ArrayList<Iterator<Entry<MemorySegment>>> iterators = new ArrayList<>();
        if (from == null && to == null) {
            iterators.add(currentState.dataStorage.values().iterator());
        } else if (from == null) {
            iterators.add(currentState.dataStorage.headMap(to).values().iterator());
        } else if (to == null) {
            iterators.add(currentState.dataStorage.tailMap(from).values().iterator());
        } else {
            iterators.add(currentState.dataStorage.subMap(from, to).values().iterator());
        }

        iterators.add(currentState.getFlushingPairsIterator());
        iterators.addAll(currentState.fileManager.createIterators(from, to));
        Iterator<Entry<MemorySegment>> mergedIterator = MergedIterator.createMergedIterator(iterators, EntryKeyComparator.INSTANCE);
        return new SkipNullIterator(mergedIterator);
    }

    @Override
    public Entry<MemorySegment> get(MemorySegment key) {
        if (isClosed.get()) {
            throw new RuntimeException("Unable to get: close operation performed.");
        }

        State currentState;
        lock.readLock().lock();
        try {
            currentState = this.state;
        } finally {
            lock.readLock().unlock();
        }

        Entry<MemorySegment> result = currentState.dataStorage.get(key);
        if (result == null) {
            result = currentState.fileManager.get(key);
        }

        return (result == null || result.value() == null) ? null : result;
    }

    @Override
    public void upsert(Entry<MemorySegment> entry) {
        if (isClosed.get()) {
            throw new RuntimeException("Unable to upsert: close operation performed.");
        }

        State currentState = currentState();
        int entryValueLength = entry.value() == null ? 0 : (int) entry.value().byteSize();
        int delta = 2 * (int) entry.key().byteSize() + entryValueLength;
        synchronized (object) {
            if (currentState.getSize().get() + delta >= flushThresholdBytes) {
                startFlushing();
            } else {
                currentState.updateSize(delta);
            }
        }

        currentState.dataStorage.put(entry.key(), entry);
    }

    @Override
    public void flush() {
        if (isClosed.get()) {
            throw new RuntimeException("Unable to flush: close operation performed.");
        }

        State currentState = this.state;
        if (currentState.dataStorage.isEmpty()) {
            return;
        }

        startFlushing();
    }

    @Override
    public void compact() {
        if (isClosed.get()) {
            throw new RuntimeException("Unable to compact: close operation performed.");
        }

        performCompact();
    }

    @Override
    public void close() throws IOException {
        if (isClosed.get()) {
            return;
        }

        if (fileManager == null) {
            return;
        }

        flush();
        performClose();
        fileManager.closeArena();
        isClosed.set(true);
    }

    private State currentState() {
        State currentState;
        lock.writeLock().lock();
        try {
            currentState = this.state;
        } finally {
            lock.writeLock().unlock();
        }

        return currentState;
    }

    private void startFlushing() {
        if (isBackgroundFlushing.get()) {
            throw new IllegalStateException("Unable to flash: tables are full.");
        }

        isBackgroundFlushing.set(true);
        performBackgroundFlush();
    }

    private void performBackgroundFlush() {
        State currentState = currentState();
        taskResults.add(service.submit(() -> {
            try {
                lock.writeLock().lock();
                try {
                    this.state = currentState.beforeFlushState();
                } finally {
                    lock.writeLock().unlock();
                }

                currentState.fileManager.flush(currentState.dataStorage);
                
                lock.writeLock().lock();
                try {
                    this.state = this.state.afterFlushState();
                } finally {
                    lock.writeLock().unlock();
                }
            } finally {
                isBackgroundFlushing.set(false);
            }
        }));
    }

    private void performCompact() {
        State currentState = currentState();
        taskResults.add(service.submit(() -> {
            currentState.fileManager.performCompact(currentState.fileManager.createIterators(null, null),
                    !currentState.dataStorage.isEmpty());
        }));
    }

    private void performClose() {
        service.shutdown();
        for (Future<?> taskResult : taskResults) {
            if (taskResult != null && !taskResult.isDone()) {
                try {
                    taskResult.get();
                } catch (ExecutionException | InterruptedException e) {
                    throw new RuntimeException("Current thread was interrupted.", e);
                }
            }
        }

        service.close();
        taskResults.clear();
    }

    private static class State {
        private final NavigableMap<MemorySegment, Entry<MemorySegment>> dataStorage;
        private final NavigableMap<MemorySegment, Entry<MemorySegment>> flushingDataStorage;
        private final AtomicInteger pairsSize;
        private final FileManager fileManager;

        private State(NavigableMap<MemorySegment, Entry<MemorySegment>> dataStorage,
                      NavigableMap<MemorySegment, Entry<MemorySegment>> flushingDataStorage,
                      FileManager fileManager) {
            this.dataStorage = dataStorage;
            this.fileManager = fileManager;
            this.flushingDataStorage = flushingDataStorage;
            pairsSize = new AtomicInteger(0);
        }

        private static State emptyState(FileManager fileManager) {
            return new State(new ConcurrentSkipListMap<>(MemorySegmentComparator.INSTANCE),
                    new ConcurrentSkipListMap<>(MemorySegmentComparator.INSTANCE), fileManager);
        }

        private State beforeFlushState() {
            return new State(new ConcurrentSkipListMap<>(MemorySegmentComparator.INSTANCE),
                    dataStorage, fileManager);
        }

        private State afterFlushState() {
            return new State(dataStorage, new ConcurrentSkipListMap<>(MemorySegmentComparator.INSTANCE),
                    fileManager);
        }

        private AtomicInteger getSize() {
            return pairsSize;
        }

        private void updateSize(int delta) {
            pairsSize.getAndAdd(delta);
        }

        private Iterator<Entry<MemorySegment>> getFlushingPairsIterator() {
            return flushingDataStorage == null ? null : flushingDataStorage.values().iterator();
        }
    }
}
