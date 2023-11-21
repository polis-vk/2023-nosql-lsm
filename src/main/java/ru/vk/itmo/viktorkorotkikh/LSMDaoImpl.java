package ru.vk.itmo.viktorkorotkikh;

import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.nio.file.Path;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;
import java.util.logging.Logger;

public class LSMDaoImpl implements Dao<MemorySegment, Entry<MemorySegment>> {

    private static final Logger LOG = Logger.getLogger(LSMDaoImpl.class.getName());

    private final AtomicReference<MemTable> memTable;

    private final AtomicReference<MemTable> flushingMemTable;

    private final AtomicBoolean isFlushing = new AtomicBoolean(false);

    private final AtomicReference<Deque<SSTable>> ssTables;

    private final AtomicInteger ssTablesIndex = new AtomicInteger(0);
    private Arena ssTablesArena;

    private final Path storagePath;

    private final long flushThresholdBytes;

    private final ExecutorService bgExecutor = Executors.newSingleThreadExecutor();

    private final ExecutorService compactionExecutor = Executors.newSingleThreadExecutor();

    public LSMDaoImpl(Path storagePath, long flushThresholdBytes) {
        this.memTable = new AtomicReference<>(new MemTable(flushThresholdBytes));
        this.flushingMemTable = new AtomicReference<>(new MemTable(-1));
        try {
            this.ssTablesArena = Arena.ofShared();
            this.ssTables = new AtomicReference<>(SSTable.load(ssTablesArena, storagePath, ssTablesIndex));
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
        List<SSTable.SSTableIterator> ssTableIterators = SSTable.ssTableIterators(ssTables.get(), from, to);
        return MergeIterator.create(
                memTable.get().iterator(from, to),
                flushingMemTable.get().iterator(from, to),
                ssTableIterators
        );
    }

    @Override
    public Entry<MemorySegment> get(MemorySegment key) {
        Entry<MemorySegment> fromMemTable = memTable.get().get(key);
        if (fromMemTable != null) {
            return fromMemTable.value() == null ? null : fromMemTable;
        }
        Entry<MemorySegment> fromFlushingMemTable = flushingMemTable.get().get(key);
        if (fromFlushingMemTable != null) {
            return fromFlushingMemTable.value() == null ? null : fromFlushingMemTable;
        }
        // reverse order because last sstable has the highest priority
        Iterator<SSTable> ssTableIterator = ssTables.get().descendingIterator();
        while (ssTableIterator.hasNext()) {
            SSTable ssTable = ssTableIterator.next();
            Entry<MemorySegment> fromDisk = ssTable.get(key);
            if (fromDisk != null) {
                return fromDisk.value() == null ? null : fromDisk;
            }
        }
        return null;
    }

    @Override
    public void upsert(Entry<MemorySegment> entry) {
        AtomicBoolean overflow = new AtomicBoolean(false);
        memTable.updateAndGet(memTable1 -> {
            overflow.set(memTable1.upsert(entry));
            return memTable1;
        });
        if (overflow.get()) { // bg flush
            if (isFlushing.getAndSet(true)) {
                throw new TooManyFlushesException();
            }
            memTable.updateAndGet(memTable1 -> {
                flushingMemTable.set(memTable1);
                return new MemTable(flushThresholdBytes);
            });
            Future<?> unused = runFlushInBackground();
        }
    }

    @Override
    public void compact() throws IOException {
        if (SSTable.isCompacted(ssTables.get())) {
            return;
        }

        Future<?> compaction = compactionExecutor.submit(this::compactInBackground);
        await(compaction);
    }

    private void compactInBackground() {
        try {
            Deque<SSTable> ssTablesToCompact = ssTables.get();
            int lastIndex = ssTablesIndex.get();
            Path compacted = SSTable.compact(
                    () -> MergeIterator.createThroughSSTables(
                            SSTable.ssTableIterators(ssTablesToCompact, null, null)
                    ),
                    storagePath
            );
            Deque<SSTable> newSSTables =
                    SSTable.replaceSSTablesWithCompacted(ssTablesArena, compacted, storagePath, ssTables.get());
            ssTablesIndex.compareAndSet(lastIndex, 1);
            while (lastIndex <= ssTablesIndex.get() - 1) {
                newSSTables.add(SSTable.loadOneByIndex(ssTablesArena, storagePath, lastIndex));
                lastIndex++;
            }
            ssTables.set(newSSTables);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void flush() throws IOException {
        if (isFlushing.getAndSet(true)) {
            return;
        }

        memTable.updateAndGet(memTable1 -> {
            flushingMemTable.set(memTable1);
            return new MemTable(flushThresholdBytes);
        });
        await(runFlushInBackground());
    }

    private Future<?> runFlushInBackground() {
        return bgExecutor.submit(() -> {
            try {
                flush(flushingMemTable, ssTablesIndex.getAndIncrement(), storagePath, ssTablesArena);
                flushingMemTable.set(new MemTable(-1));
                isFlushing.set(false);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
    }

    private void flush(
            AtomicReference<MemTable> memTable,
            int fileIndex,
            Path storagePath,
            Arena ssTablesArena
    ) throws IOException {
        if (memTable.get().isEmpty()) return;
        SSTable.save(memTable.get(), fileIndex, storagePath);
        ssTables.get().add(SSTable.loadOneByIndex(ssTablesArena, storagePath, fileIndex));
        flushingMemTable.set(new MemTable(flushThresholdBytes));
    }

    private void await(Future<?> future) {
        try {
            future.get();
        } catch (InterruptedException e) {
            LOG.log(Level.SEVERE, STR. "InterruptedException: \{ e }" );
            Thread.currentThread().interrupt();
        } catch (ExecutionException e) {
            throw new RuntimeException(e.getCause());
        }
    }

    @Override
    public void close() throws IOException {
        if (ssTablesArena.scope().isAlive()) {
            ssTablesArena.close();
        }
        bgExecutor.shutdown();
        compactionExecutor.shutdown();
        try {
            for (; ; ) {
                if (bgExecutor.awaitTermination(10, TimeUnit.MINUTES)) {
                    break;
                }
            }
            for (; ; ) {
                if (compactionExecutor.awaitTermination(10, TimeUnit.MINUTES)) {
                    break;
                }
            }
        } catch (InterruptedException e) {
            LOG.log(Level.SEVERE, STR. "InterruptedException: \{ e }" );
            Thread.currentThread().interrupt();
        }
        SSTable.save(memTable.get(), ssTablesIndex.getAndIncrement(), storagePath);
    }
}
