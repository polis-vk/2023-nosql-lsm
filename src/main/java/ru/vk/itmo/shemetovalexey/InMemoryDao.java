package ru.vk.itmo.shemetovalexey;

import ru.vk.itmo.Config;
import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;
import ru.vk.itmo.shemetovalexey.sstable.SSTable;
import ru.vk.itmo.shemetovalexey.sstable.SSTableIterator;
import ru.vk.itmo.shemetovalexey.sstable.SSTableStates;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.io.UncheckedIOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class InMemoryDao implements Dao<MemorySegment, Entry<MemorySegment>> {
    private final ExecutorService bgExecutor = Executors.newSingleThreadExecutor();
    private final AtomicReference<SSTableStates> sstableState;
    private final Arena arena;
    private final Path path;
    private final AtomicLong size = new AtomicLong();
    private final long maxSize;
    private final AtomicBoolean closed = new AtomicBoolean();
    private final ReadWriteLock upsertLock = new ReentrantReadWriteLock();

    public InMemoryDao(Config config) throws IOException {
        maxSize = config.flushThresholdBytes() == 0 ? config.flushThresholdBytes() : Long.MAX_VALUE / 2;
        path = config.basePath().resolve("data");
        Files.createDirectories(path);

        arena = Arena.ofShared();

        List<MemorySegment> segments = SSTable.loadOrRecover(path, arena);
        sstableState = new AtomicReference<>(SSTableStates.create(segments));
    }

    public static int compare(MemorySegment memorySegment1, MemorySegment memorySegment2) {
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
        SSTableStates state = this.sstableState.get();
        return SSTableIterator.get(
            getInMemory(state.getReadStorage(), from, to),
            getInMemory(state.getWriteStorage(), from, to),
            state.getDiskSegmentList(),
            from,
            to
        );
    }

    private Iterator<Entry<MemorySegment>> getInMemory(
        ConcurrentSkipListMap<MemorySegment, Entry<MemorySegment>> storage,
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
        upsertLock.readLock().lock();
        try {
            sstableState.get().getWriteStorage().put(entry.key(), entry);
            long keySize = entry.key().byteSize();
            long valueSize = entry.value() == null ? 0 : entry.value().byteSize();
            if (size.addAndGet(keySize + valueSize) >= maxSize) {
                flush();
                size.set(0);
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } finally {
            upsertLock.readLock().unlock();
        }
    }

    @Override
    public Entry<MemorySegment> get(MemorySegment key) {
        SSTableStates state = this.sstableState.get();
        Entry<MemorySegment> entry = state.getWriteStorage().get(key);
        if (entry != null) {
            if (entry.value() == null) {
                return null;
            }
            return entry;
        }

        entry = state.getReadStorage().get(key);
        if (entry != null) {
            if (entry.value() == null) {
                return null;
            }
            return entry;
        }

        Iterator<Entry<MemorySegment>> iterator = SSTableIterator.get(state.getDiskSegmentList(), key);
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
    public void compact() {
        bgExecutor.execute(() -> {
            try {
                SSTableStates state = this.sstableState.get();
                MemorySegment newPage = SSTable.compact(
                    arena,
                    path,
                    () -> SSTableIterator.get(state.getDiskSegmentList())
                );
                this.sstableState.set(state.compact(newPage));
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        });
    }

    @Override
    public void flush() throws IOException {
        bgExecutor.execute(() -> {
            Collection<Entry<MemorySegment>> entries;
            SSTableStates prevState = sstableState.get();
            ConcurrentSkipListMap<MemorySegment, Entry<MemorySegment>> writeStorage = prevState.getWriteStorage();
            if (writeStorage.isEmpty()) {
                return;
            }

            SSTableStates nextState = prevState.beforeFlush();

            upsertLock.writeLock().lock();
            try {
                sstableState.set(nextState);
            } finally {
                upsertLock.writeLock().unlock();
            }

            entries = writeStorage.values();
            MemorySegment newPage;
            try {
                newPage = SSTable.saveNextSSTable(arena, path, entries);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }

            nextState = nextState.afterFlush(newPage);
            upsertLock.writeLock().lock();
            try {
                sstableState.set(nextState);
            } finally {
                upsertLock.writeLock().unlock();
            }
        });
    }

    @Override
    public void close() throws IOException {
        if (closed.getAndSet(true)) {
            waitForClose();
            return;
        }

        flush();
        bgExecutor.execute(arena::close);
        bgExecutor.shutdown();
        waitForClose();
    }

    private void waitForClose() throws InterruptedIOException {
        try {
            if (!bgExecutor.awaitTermination(1, TimeUnit.MINUTES)) {
                throw new InterruptedIOException();
            }
        } catch (InterruptedException e) {
            InterruptedIOException exception = new InterruptedIOException("Interrupted or timed out");
            exception.initCause(e);
            throw exception;
        }
    }
}
