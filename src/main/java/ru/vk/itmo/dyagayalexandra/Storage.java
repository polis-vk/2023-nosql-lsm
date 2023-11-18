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
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class Storage implements Dao<MemorySegment, Entry<MemorySegment>> {

    private FileManager fileManager;
    private final AtomicBoolean isClosed = new AtomicBoolean();
    private final List<Future<?>> taskResults = new CopyOnWriteArrayList<>();
    private final ExecutorService service =
            Executors.newSingleThreadExecutor(r -> new Thread(r, "BackgroundFlushAndCompact"));
    private long flushThresholdBytes;
    private State state;
    private static final MemorySegmentComparator memorySegmentComparator = new MemorySegmentComparator();
    private final EntryKeyComparator entryKeyComparator = new EntryKeyComparator(memorySegmentComparator);
    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    public Storage() {
    }

    public Storage(Config config) {
        fileManager = new FileManager(config, memorySegmentComparator, entryKeyComparator);
        state = State.emptyState(fileManager);
        flushThresholdBytes = config.flushThresholdBytes();
    }

    @Override
    public Iterator<Entry<MemorySegment>> get(MemorySegment from, MemorySegment to) {
        if (isClosed.get()) {
            throw new RuntimeException("Unable to get: close operation performed.");
        }

        ArrayList<Iterator<Entry<MemorySegment>>> iterators = new ArrayList<>();
        if (from == null && to == null) {
            iterators.add(state.dataStorage.values().iterator());
        } else if (from == null) {
            iterators.add(state.dataStorage.headMap(to).values().iterator());
        } else if (to == null) {
            iterators.add(state.dataStorage.tailMap(from).values().iterator());
        } else {
            iterators.add(state.dataStorage.subMap(from, to).values().iterator());
        }

        iterators.add(state.getFlushingPairsIterator());
        iterators.addAll(state.fileManager.createIterators(from, to));
        Iterator<Entry<MemorySegment>> mergedIterator =
                MergedIterator.createMergedIterator(iterators, entryKeyComparator);
        return new SkipNullIterator(mergedIterator);
    }

    @Override
    public Entry<MemorySegment> get(MemorySegment key) {
        if (isClosed.get()) {
            throw new RuntimeException("Unable to get: close operation performed.");
        }

        Entry<MemorySegment> result = state.dataStorage.get(key);
        if (result == null) {
            result = state.fileManager.get(key);
        }

        return (result == null || result.value() == null) ? null : result;
    }

    @Override
    public void upsert(Entry<MemorySegment> entry) {
        if (isClosed.get()) {
            throw new RuntimeException("Unable to upsert: close operation performed.");
        }

        if (entry == null || entry.key() == null) {
            throw new IllegalArgumentException("Attempt to upsert empty entry.");
        }

        int entryValueLength = entry.value() == null ? 0 : (int) entry.value().byteSize();
        int delta = 2 * (int) entry.key().byteSize() + entryValueLength;

        if (state.dataStorage.size() + delta >= flushThresholdBytes) {
            if (!state.flushingDataStorage.isEmpty()) {
                throw new RuntimeException("Unable to flush: another background flush performing.");
            }

            flush();
        }

        lock.readLock().lock();
        try {
            state.dataStorage.put(entry.key(), entry);
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public void flush() {
        if (isClosed.get()) {
            throw new RuntimeException("Unable to flush: close operation performed.");
        }

        if (state.dataStorage.isEmpty() || !state.flushingDataStorage.isEmpty()) {
            return;
        }

        performBackgroundFlush();
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
            isClosed.set(true);
            return;
        }

        performClose();
        fileManager.closeArena();
        isClosed.set(true);
    }

    private void performBackgroundFlush() {
        lock.writeLock().lock();
        try {
            state = state.beforeFlushState();
        } finally {
            lock.writeLock().unlock();
        }

        taskResults.add(service.submit(() -> {
            state.fileManager.flush(state.flushingDataStorage.values());

            lock.writeLock().lock();
            try {
                state = state.afterFlushState();
            } finally {
                lock.writeLock().unlock();
            }
        }));
    }

    private void performCompact() {
        taskResults.add(service.submit(state.fileManager::performCompact));
    }

    private void performClose() {
        for (Future<?> taskResult : taskResults) {
            if (taskResult != null && !taskResult.isDone()) {
                try {
                    taskResult.get();
                } catch (ExecutionException | InterruptedException e) {
                    throw new RuntimeException("Current thread was interrupted.", e);
                }
            }
        }

        flush();

        service.close();
        taskResults.clear();
    }

    private static class State {
        private final NavigableMap<MemorySegment, Entry<MemorySegment>> dataStorage;
        private final NavigableMap<MemorySegment, Entry<MemorySegment>> flushingDataStorage;
        private final FileManager fileManager;

        private State(NavigableMap<MemorySegment, Entry<MemorySegment>> dataStorage,
                      NavigableMap<MemorySegment, Entry<MemorySegment>> flushingDataStorage,
                      FileManager fileManager) {
            this.dataStorage = dataStorage;
            this.fileManager = fileManager;
            this.flushingDataStorage = flushingDataStorage;
        }

        private static State emptyState(FileManager fileManager) {
            return new State(new ConcurrentSkipListMap<>(memorySegmentComparator),
                    new ConcurrentSkipListMap<>(memorySegmentComparator), fileManager);
        }

        private State beforeFlushState() {
            return new State(new ConcurrentSkipListMap<>(memorySegmentComparator), dataStorage, fileManager);
        }

        private State afterFlushState() {
            return new State(dataStorage, new ConcurrentSkipListMap<>(memorySegmentComparator), fileManager);
        }

        private Iterator<Entry<MemorySegment>> getFlushingPairsIterator() {
            return flushingDataStorage == null ? null : flushingDataStorage.values().iterator();
        }
    }
}
