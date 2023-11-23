package ru.vk.itmo.test.ryabovvadim.memtable;

import ru.vk.itmo.Entry;
import ru.vk.itmo.test.ryabovvadim.exception.MemoryTableOutOfMemoryException;
import ru.vk.itmo.test.ryabovvadim.iterators.MemoryMergeIterators;
import ru.vk.itmo.test.ryabovvadim.sstable.SSTableManager;
import ru.vk.itmo.test.ryabovvadim.utils.IteratorUtils;
import ru.vk.itmo.test.ryabovvadim.utils.MemorySegmentUtils;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.util.Iterator;
import java.util.NavigableMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;
import java.util.logging.Logger;

public class MemoryTable {
    private final Logger log = Logger.getLogger(MemoryTable.class.getName());

    private final AtomicReference<ConcurrentNavigableMap<MemorySegment, Entry<MemorySegment>>> memTable =
            new AtomicReference<>(createMap());
    private final AtomicReference<ConcurrentNavigableMap<MemorySegment, Entry<MemorySegment>>> flushTable =
            new AtomicReference<>(null);
    private final ExecutorService flushWorker = Executors.newSingleThreadExecutor();
    private final SSTableManager ssTableManager;
    private final long flushThresholdBytes;
    private AtomicLong usedSpace = new AtomicLong();
    private AtomicBoolean wasDropped = new AtomicBoolean(true);
    private volatile Future<?> flushFuture = CompletableFuture.completedFuture(null);

    public MemoryTable(SSTableManager ssTableManager, long flushThresholdBytes) {
        this.ssTableManager = ssTableManager;
        this.flushThresholdBytes = flushThresholdBytes;
    }

    public Entry<MemorySegment> get(MemorySegment key) {
        Entry<MemorySegment> entry = memTable.get().get(key);
        if (entry == null) {
            NavigableMap<MemorySegment, Entry<MemorySegment>> curFlushTable = flushTable.get();
            if (curFlushTable != null) {
                entry = curFlushTable.get(key);
            }
        }

        return entry;
    }

    public Iterator<Entry<MemorySegment>> get(MemorySegment from, MemorySegment to) {
        return new MemoryMergeIterators(
                getIterator(memTable.get(), from, to),
                getIterator(flushTable.get(), from, to)
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
        ChangeableEntryWithLock<MemorySegment> prev = (ChangeableEntryWithLock<MemorySegment>) memTable.get()
                .putIfAbsent(entry.key(), new ChangeableEntryWithLock<>(entry));

        if (prev != null) {
            prev.lock();
            try {
                long prevSize = getEntrySize(prev);
                long newSize = getEntrySize(entry);
                prev.setValue(entry.value());
                usedSpace.addAndGet(newSize - prevSize);
            } finally {
                prev.unlock();
            }
        } else {
            usedSpace.addAndGet(getEntrySize(entry));
        }

        if (usedSpace.get() > flushThresholdBytes && wasDropped.compareAndSet(true, false)) {
            if (!flushFuture.isDone()) {
                throw new MemoryTableOutOfMemoryException();
            }
            flush(false);
        }
    }

    private long getEntrySize(Entry<MemorySegment> entry) {
        if (entry == null) {
            return 0;
        }
        long size = entry.key().byteSize();
        if (entry.value() != null) {
            size += entry.value().byteSize();
        }

        return size;
    }

    public void flush(boolean importantFlush) {
        if (existsSSTableManager() && !importantFlush && (memTable.get().isEmpty() || !flushFuture.isDone())) {
            return;
        }

        flushFuture = flushWorker.submit(() -> {
            flushTable.set(memTable.get());
            memTable.set(createMap());
            usedSpace.set(0);
            wasDropped.set(true);

            try {
                ssTableManager.saveEntries(() -> flushTable.get().values().iterator());
            } catch (IOException e) {
                log.log(Level.WARNING, "Flushing was failed", e);
            } finally {
                flushTable.set(null);
            }
        });
    }

    public void close() throws IOException {
        try {
            if (!flushWorker.isShutdown()) {
                flush(true);
                flushFuture.get();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (ExecutionException e) {
            if (e.getCause() instanceof IOException ioEx) {
                log.log(Level.WARNING, "Closing was failed", e);
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
