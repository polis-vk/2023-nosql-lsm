package ru.vk.itmo.viktorkorotkikh;

import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class LSMDaoImpl implements Dao<MemorySegment, Entry<MemorySegment>> {

    private volatile MemTable memTable;

    private volatile MemTable flushingMemTable;

    private volatile Future<?> flushFuture;

    private volatile Future<?> compactionFuture;

    private volatile List<SSTable> ssTables;

    private final AtomicInteger ssTablesIndex = new AtomicInteger(0);
    private Arena ssTablesArena;

    private final Path storagePath;

    private final long flushThresholdBytes;

    private final ExecutorService bgExecutor = Executors.newSingleThreadExecutor();

    private final ReadWriteLock upsertLock = new ReentrantReadWriteLock();

    private final ReadWriteLock compactionLock = new ReentrantReadWriteLock();

    public LSMDaoImpl(Path storagePath, long flushThresholdBytes) {
        this.memTable = new MemTable(flushThresholdBytes);
        this.flushingMemTable = new MemTable(-1);
        try {
            this.ssTablesArena = Arena.ofShared();
            this.ssTables = SSTable.load(ssTablesArena, storagePath, ssTablesIndex);
        } catch (IOException e) {
            ssTablesArena.close();
            throw new LSMDaoCreationException(e);
        }
        this.storagePath = storagePath;
        this.flushThresholdBytes = flushThresholdBytes;
    }

    @Override
    public Iterator<Entry<MemorySegment>> get(MemorySegment from, MemorySegment to) {
        return mergeIterator(from, to);
    }

    private MergeIterator.MergeIteratorWithTombstoneFilter mergeIterator(MemorySegment from, MemorySegment to) {
        List<SSTable.SSTableIterator> ssTableIterators = SSTable.ssTableIterators(ssTables, from, to);
        return MergeIterator.create(
                memTable.iterator(from, to, 0),
                flushingMemTable.iterator(from, to, 1),
                ssTableIterators
        );
    }

    @Override
    public Entry<MemorySegment> get(MemorySegment key) {
        Entry<MemorySegment> fromMemTable = memTable.get(key);
        if (fromMemTable != null) {
            return fromMemTable.value() == null ? null : fromMemTable;
        }
        Entry<MemorySegment> fromFlushingMemTable = flushingMemTable.get(key);
        if (fromFlushingMemTable != null) {
            return fromFlushingMemTable.value() == null ? null : fromFlushingMemTable;
        }
        return getFromDisk(key);
    }

    private Entry<MemorySegment> getFromDisk(MemorySegment key) {
        for (int i = ssTables.size() - 1; i >= 0; i--) { // reverse order because last sstable has the highest priority
            SSTable ssTable = ssTables.get(i);
            Entry<MemorySegment> fromDisk = ssTable.get(key);
            if (fromDisk != null) {
                return fromDisk.value() == null ? null : fromDisk;
            }
        }
        return null;
    }

    @Override
    public void upsert(Entry<MemorySegment> entry) {
        upsertLock.readLock().lock();
        try {
            if (!memTable.upsert(entry)) { // no overflow
                return;
            }
        } finally {
            upsertLock.readLock().unlock();
        }
        tryToFlush(false);
    }

    @Override
    public void compact() throws IOException {
        if (SSTable.isCompacted(ssTables)) {
            return;
        }

        compactionFuture = bgExecutor.submit(this::compactInBackground);
    }

    private void compactInBackground() {
        try {
            compactionLock.writeLock().lock();
            try {
                SSTable.compact(
                        () -> MergeIterator.createThroughSSTables(
                                SSTable.ssTableIterators(ssTables, null, null)
                        ),
                        storagePath
                );
                ssTables = SSTable.load(ssTablesArena, storagePath, ssTablesIndex);
            } finally {
                compactionLock.writeLock().unlock();
            }
        } catch (IOException e) {
            throw new CompactionException(e);
        }
    }

    @Override
    public void flush() throws IOException {
        tryToFlush(true);
    }

    private void tryToFlush(boolean tolerateToBackgroundFlushing) {
        upsertLock.writeLock().lock();
        try {
            if (!flushingMemTable.isEmpty()) {
                if (tolerateToBackgroundFlushing) {
                    return;
                } else {
                    throw new TooManyFlushesException();
                }
            }
        } finally {
            upsertLock.writeLock().unlock();
        }
        flushFuture = runFlushInBackground();
    }

    private void prepareFlush() {
        upsertLock.writeLock().lock();
        try {
            flushingMemTable = memTable;
            memTable = new MemTable(flushThresholdBytes);
        } finally {
            upsertLock.writeLock().unlock();
        }
    }

    private Future<?> runFlushInBackground() {
        prepareFlush();
        return bgExecutor.submit(() -> {
            try {
                compactionLock.writeLock().lock();
                try {
                    flush(flushingMemTable, ssTablesIndex.getAndIncrement(), storagePath, ssTablesArena);
                } finally {
                    compactionLock.writeLock().lock();
                }
            } catch (IOException e) {
                throw new FlushingException(e);
            }
        });
    }

    private void flush(
            MemTable memTable,
            int fileIndex,
            Path storagePath,
            Arena ssTablesArena
    ) throws IOException {
        if (memTable.isEmpty()) return;
        SSTable.save(memTable, fileIndex, storagePath);
        ssTables = SSTable.load(ssTablesArena, storagePath, ssTablesIndex);
        flushingMemTable = new MemTable(-1);
    }

    private void await(Future<?> future) {
        try {
            future.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (ExecutionException e) {
            throw new BackgroundExecutionException(e);
        }
    }

    @Override
    public void close() throws IOException {
        bgExecutor.shutdown();
        try {
            if (flushFuture != null) {
                await(flushFuture);
            }
            if (compactionFuture != null) {
                await(compactionFuture);
            }
            bgExecutor.awaitTermination(1, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        if (ssTablesArena.scope().isAlive()) {
            ssTablesArena.close();
        }
        SSTable.save(memTable, ssTablesIndex.getAndIncrement(), storagePath);
    }
}
