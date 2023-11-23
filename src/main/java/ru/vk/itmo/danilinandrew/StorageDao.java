package ru.vk.itmo.danilinandrew;

import ru.vk.itmo.Config;
import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class StorageDao implements Dao<MemorySegment, Entry<MemorySegment>> {
    private final Arena arena;
    private final AtomicBoolean isFlushing = new AtomicBoolean();
    private final DiskStorage diskStorage;
    private final Lock storageLock = new ReentrantLock();
    private final Path path;
    private final long flushThresholdBytes;
    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    private final ExecutorService executorService = Executors.newSingleThreadExecutor();
    private MemoryState memoryState = MemoryState.newMemoryState();
    private Future<?> flushFuture;
    private Future<?> compactFuture;

    public StorageDao(Config config) throws IOException {
        this.path = config.basePath().resolve("data");
        Files.createDirectories(path);
        arena = Arena.ofShared();

        this.flushThresholdBytes = config.flushThresholdBytes();
        this.diskStorage = new DiskStorage(DiskStorageExtension.loadOrRecover(path, arena));
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
        MemoryState memory = this.memoryState;
        List<Iterator<Entry<MemorySegment>>> iterators = new ArrayList<>();
        iterators.add(Utils.getIterFrom(memory.memory, from, to));
        if (memory.flushing != null && isFlushing.get()) {
            iterators.add(Utils.getIterFrom(memory.flushing, from, to));
        }
        return diskStorage.range(iterators, from, to);
    }

    @Override
    public Entry<MemorySegment> get(MemorySegment key) {
        MemoryState state = this.memoryState;

        Entry<MemorySegment> entry = state.memory.get(key);
        if (entry != null) {
            if (entry.value() == null) {
                return null;
            }
            return entry;
        }

        if (state.flushing != null) {
            entry = state.flushing.get(key);
            if (entry != null) {
                return entry;
            }
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
    public void upsert(Entry<MemorySegment> entry) {
        lock.readLock().lock();
        try {
            Entry<MemorySegment> prev = memoryState.memory.get(entry.key());
            long prevByteSize = (prev == null) ? 0 : Utils.getByteSize(prev);
            long delta = Utils.getByteSize(entry) - prevByteSize;
            long curByteSizeUsage = memoryState.memoryUsage.addAndGet(delta);

            if (curByteSizeUsage > flushThresholdBytes && !isFlushing.getAndSet(true)) {
                flushFuture = executorService.submit(this::flushMemory);
            }

            memoryState.memory.put(entry.key(), entry);
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public void compact() throws IOException {
        if (diskStorage == null) {
            return;
        }

        storageLock.lock();
        try {
            compactFuture = executorService.submit(() -> {
                try {
                    DiskStorage.compact(
                            path,
                            this::all
                    );
                } catch (IOException e) {
                    throw new IllegalStateException("Failed to compact", e);
                }
            });
        } finally {
            storageLock.unlock();
        }
    }

    @Override
    public void flush() throws IOException {
        flushMemory();
    }

    @Override
    public void close() throws IOException {
        if (diskStorage == null) {
            return;
        }

        try {
            if (compactFuture != null) {
                compactFuture.get();
            }
            if (flushFuture != null) {
                flushFuture.get();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (ExecutionException e) {
            throw new IllegalStateException("Dao can not be stopped gracefully", e);
        }
        executorService.close();

        if (!memoryState.memory.isEmpty()) {
            diskStorage.saveNextSSTable(path, memoryState.memory.values(), arena);
        }

        if (!arena.scope().isAlive()) {
            return;
        }
        arena.close();
    }

    private void flushMemory() {
        if (memoryState.memory.isEmpty()) {
            return;
        }

        if (diskStorage == null) {
            return;
        }
        storageLock.lock();
        try {
            lock.writeLock().lock();
            try {
                this.memoryState = memoryState.prepareForFlush();
            } finally {
                lock.writeLock().unlock();
            }

            diskStorage.saveNextSSTable(
                    path,
                    memoryState.flushing.values(),
                    arena
            );

            lock.writeLock().lock();
            try {
                this.memoryState = memoryState.afterFlush();
            } finally {
                lock.writeLock().unlock();
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } finally {
            isFlushing.set(false);
            storageLock.unlock();
        }
    }

    private static class MemoryState {
        final ConcurrentNavigableMap<MemorySegment, Entry<MemorySegment>> memory;
        final ConcurrentNavigableMap<MemorySegment, Entry<MemorySegment>> flushing;
        final AtomicLong memoryUsage;

        private MemoryState(ConcurrentNavigableMap<MemorySegment, Entry<MemorySegment>> memory,
                            ConcurrentNavigableMap<MemorySegment, Entry<MemorySegment>> flushing) {
            this.memory = memory;
            this.flushing = flushing;
            this.memoryUsage = new AtomicLong();
        }

        private MemoryState(ConcurrentNavigableMap<MemorySegment, Entry<MemorySegment>> memory,
                            ConcurrentNavigableMap<MemorySegment, Entry<MemorySegment>> flushing,
                            AtomicLong memoryUsage) {
            this.memory = memory;
            this.flushing = flushing;
            this.memoryUsage = memoryUsage;
        }

        static MemoryState newMemoryState() {
            return new MemoryState(
                    new ConcurrentSkipListMap<>(StorageDao::compare),
                    new ConcurrentSkipListMap<>(StorageDao::compare)
            );
        }

        MemoryState prepareForFlush() {
            return new MemoryState(
                    new ConcurrentSkipListMap<>(StorageDao::compare),
                    memory,
                    memoryUsage
            );
        }

        MemoryState afterFlush() {
            return new MemoryState(
                    memory,
                    new ConcurrentSkipListMap<>(StorageDao::compare)
            );
        }

    }
}
