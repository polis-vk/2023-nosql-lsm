package ru.vk.itmo.test.ryabovvadim.memtable;

import ru.vk.itmo.Entry;
import ru.vk.itmo.test.ryabovvadim.exception.MemoryTableOutOfMemoryException;
import ru.vk.itmo.test.ryabovvadim.sstable.SSTableManager;
import ru.vk.itmo.test.ryabovvadim.utils.IteratorUtils;
import ru.vk.itmo.test.ryabovvadim.utils.MemorySegmentUtils;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.util.Iterator;
import java.util.NavigableMap;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class MemoryTable {
    private volatile ConcurrentNavigableMap<MemorySegment, Entry<MemorySegment>> memTable = createMap();
    private volatile ConcurrentNavigableMap<MemorySegment, Entry<MemorySegment>> flushTable = null;
    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    private final ExecutorService flushWorker = Executors.newSingleThreadExecutor();
    private final SSTableManager ssTableManager;
    private final long flushThresholdBytes;
    private AtomicLong usedSpace = new AtomicLong();
    private Future<?> flushFuture;

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
        return mergeIterators(
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

    private Iterator<Entry<MemorySegment>> mergeIterators(
            Iterator<Entry<MemorySegment>> fstIterator,
            Iterator<Entry<MemorySegment>> sndIterator
    ) {
        return new Iterator<>() {
            private Entry<MemorySegment> fstEntry;
            private Entry<MemorySegment> sndEntry;

            @Override
            public boolean hasNext() {
                return fstIterator.hasNext() ||
                        sndIterator.hasNext() ||
                        fstEntry != null ||
                        sndEntry != null;
            }

            @Override
            public Entry<MemorySegment> next() {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }

                if (fstEntry == null && fstIterator.hasNext()) {
                    fstEntry = fstIterator.next();
                }
                if (sndEntry == null && sndIterator.hasNext()) {
                    sndEntry = sndIterator.next();
                }

                Entry<MemorySegment> result;
                if (fstEntry == null) {
                    result = sndEntry;
                    sndEntry = null;
                } else if (sndEntry == null) {
                    result = fstEntry;
                    fstEntry = null;
                } else {
                    int compareResult = MemorySegmentUtils.compareMemorySegments(fstEntry.key(), sndEntry.key());
                    if (compareResult < 0) {
                        result = fstEntry;
                        fstEntry = null;
                    } else if (compareResult == 0) {
                        result = fstEntry;
                        fstEntry = null;
                        sndEntry = null;
                    } else {

                        result = sndEntry;
                        sndEntry = null;
                    }
                }

                return result;
            }
        };
    }

    public void upsert(Entry<MemorySegment> entry) {
        if (flushTable != null && usedSpace.get() > flushThresholdBytes) {
            throw new MemoryTableOutOfMemoryException();
        }

        lock.readLock().lock();
        try {
            Entry<MemorySegment> oldEntry = memTable.put(entry.key(), entry);
            long newValueSize = entry.value() == null ? 0 : entry.value().byteSize();
            long oldValueSize = oldEntry == null || oldEntry.value() == null ? 0 : oldEntry.value().byteSize();

            if (usedSpace.updateAndGet(x -> x + (newValueSize - oldValueSize)) < flushThresholdBytes) {
                return;
            }
        } finally {
            lock.readLock().unlock();
        }
        flush();
    }

    public void flush() {
        if (!existsSSTableManager()) {
            return;
        }

        flushFuture = flushWorker.submit(() -> {
            if (memTable.isEmpty()) {
                return;
            }

            lock.writeLock().lock();
            try {
                flushTable = memTable;
                memTable = createMap();
                usedSpace.set(0);
            } finally {
                lock.writeLock().unlock();
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
        } catch (InterruptedException ignored) {
            // Ignored exception
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
