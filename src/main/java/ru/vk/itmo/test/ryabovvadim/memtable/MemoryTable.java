package ru.vk.itmo.test.ryabovvadim.memtable;

import ru.vk.itmo.Entry;
import ru.vk.itmo.test.ryabovvadim.exception.MemoryTableOutOfMemoryException;
import ru.vk.itmo.test.ryabovvadim.iterators.MemoryMergeIterators;
import ru.vk.itmo.test.ryabovvadim.sstable.SSTableManager;
import ru.vk.itmo.test.ryabovvadim.utils.IteratorUtils;
import ru.vk.itmo.test.ryabovvadim.utils.MemorySegmentUtils;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.foreign.MemorySegment;
import java.util.Iterator;
import java.util.NavigableMap;
import java.util.NoSuchElementException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class MemoryTable {
    private volatile ConcurrentNavigableMap<MemorySegment, Entry<MemorySegment>> memTable = createMap();
    private volatile ConcurrentNavigableMap<MemorySegment, Entry<MemorySegment>> flushTable = null;
    private final Lock lock = new ReentrantLock();
    private final ExecutorService flushWorker = Executors.newSingleThreadExecutor();
    private final SSTableManager ssTableManager;
    private final long flushThresholdBytes;
    private AtomicLong usedSpace = new AtomicLong();
    private Future<?> flushFuture = CompletableFuture.completedFuture(null);

    public MemoryTable(SSTableManager ssTableManager, long flushThresholdBytes) {
        this.ssTableManager = ssTableManager;
        this.flushThresholdBytes = flushThresholdBytes;
    }

    public Entry<MemorySegment> get(MemorySegment key) {
        Entry<MemorySegment> entry = memTable.get(key);
        if (entry == null) {
            NavigableMap<MemorySegment, Entry<MemorySegment>> curFlushTable = flushTable;
            if (curFlushTable != null) {
                entry = curFlushTable.get(key);
            }
        }

        return entry;
    }

    public Iterator<Entry<MemorySegment>> get(MemorySegment from, MemorySegment to) {
        return new MemoryMergeIterators(
                getIterator(memTable, from, to),
                getIterator(flushTable, from, to)
        );
    }

    private Iterator<Entry<MemorySegment>> getIterator(
            ConcurrentNavigableMap<MemorySegment, Entry<MemorySegment>> table,
            MemorySegment from,
            MemorySegment to
    ) {
        if (table == null) {
            return IteratorUtils.emptyIterator();
        }

        if (from == null && to == null) {
            return table.values().iterator();
        } else if (from == null) {
            return table.headMap(to).values().iterator();
        } else if (to == null) {
            return table.tailMap(from).values().iterator();
        }
        return table.subMap(from, to).values().iterator();
    }

    public void upsert(Entry<MemorySegment> entry) {
        lock.lock();
        try {
            Entry<MemorySegment> oldEntry = memTable.put(entry.key(), entry);
            long newValueSize = entry.value() == null ? 0 : entry.value().byteSize();
            long oldValueSize = oldEntry == null || oldEntry.value() == null ? 0 : oldEntry.value().byteSize();

            if (usedSpace.addAndGet(newValueSize - oldValueSize) < flushThresholdBytes) {
                return;
            }
        } finally {
            lock.unlock();
        }

        if (flushTable != null) {
            throw new MemoryTableOutOfMemoryException();
        }
        flush();
    }

    public void flush() {
        if (!existsSSTableManager() || memTable.isEmpty() || !flushFuture.isDone()) {
            return;
        }

        flushFuture = flushWorker.submit(() -> {
            lock.lock();
            try {
                flushTable = memTable;
                memTable = createMap();
                usedSpace.set(0);
            } finally {
                lock.unlock();
            }

            try {
                ssTableManager.saveEntries(() -> flushTable.values().iterator());
            } catch (IOException ignored) {
                // Ignored exception
            } finally {
                flushTable = null;
            }
        });
    }

    public void close() throws IOException {
        try {
            if (!flushWorker.isShutdown()) {
                flush();
                flushFuture.get();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (ExecutionException e) {
            if (e.getCause() instanceof IOException ioEx) {
                throw ioEx;
            }
        } finally {
            flushWorker.close();
        }
    }

    private boolean existsSSTableManager() {
        return ssTableManager != null;
    }

    private static ConcurrentNavigableMap<MemorySegment, Entry<MemorySegment>> createMap() {
        return new ConcurrentSkipListMap<>(MemorySegmentUtils::compareMemorySegments);
    }
}
