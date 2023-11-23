package ru.vk.itmo.test.ryabovvadim.memtable;

import ru.vk.itmo.Entry;
import ru.vk.itmo.test.ryabovvadim.exception.MemoryTableOutOfMemoryException;
import ru.vk.itmo.test.ryabovvadim.sstable.SSTableManager;
import ru.vk.itmo.test.ryabovvadim.utils.MemorySegmentUtils;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;
import java.util.logging.Logger;

public class MemoryTable {
    private final Logger log = Logger.getLogger(MemoryTable.class.getName());

    private final AtomicReference<ConcurrentNavigableMap<MemorySegment, Entry<MemorySegment>>> refMemoryTable =
            new AtomicReference<>(createMap());
    private final AtomicReference<ConcurrentNavigableMap<MemorySegment, Entry<MemorySegment>>> refFlushTable =
            new AtomicReference<>(createMap());
    private final ExecutorService flushWorker = Executors.newSingleThreadExecutor();
    private final SSTableManager ssTableManager;
    private final long flushThresholdBytes;
    private final AtomicLong usedSpace = new AtomicLong();

    public MemoryTable(SSTableManager ssTableManager, long flushThresholdBytes) {
        this.ssTableManager = ssTableManager;
        this.flushThresholdBytes = flushThresholdBytes;
    }

    public Entry<MemorySegment> get(MemorySegment key) {
        return refMemoryTable.get().getOrDefault(key, refFlushTable.get().get(key));
    }

    public Iterator<Entry<MemorySegment>> get(MemorySegment from, MemorySegment to) {
        return mergeIterators(
                getIterator(refMemoryTable, from, to),
                getIterator(refFlushTable, from, to)
        );
    }

    private Iterator<Entry<MemorySegment>> getIterator(
            AtomicReference<ConcurrentNavigableMap<MemorySegment, Entry<MemorySegment>>> table,
            MemorySegment from,
            MemorySegment to
    ) {
        if (from == null && to == null) {
            return table.get().values().iterator();
        } else if (from == null) {
            return table.get().headMap(to).values().iterator();
        } else if (to == null) {
            return table.get().tailMap(from).values().iterator();
        }
        return table.get().subMap(from, to).values().iterator();
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
                return fstIterator.hasNext() || sndIterator.hasNext() || fstEntry != null || sndEntry != null;
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

                int compareResult = MemorySegmentUtils.compareMemorySegments(
                        fstEntry == null ? null : fstEntry.key(),
                        sndEntry == null ? null : sndEntry.key()
                );
                Entry<MemorySegment> result;
                if (compareResult < 0 && fstEntry != null) {
                    result = fstEntry;
                    fstEntry = null;
                } else if (compareResult > 0 && sndEntry != null){
                    result = sndEntry;
                    sndEntry = null;
                } else {
                    result = fstEntry;
                    fstEntry = null;
                    sndEntry = null;
                }

                return result;
            }
        };
    }

    public void upsert(Entry<MemorySegment> entry) {
        ConcurrentNavigableMap<MemorySegment, Entry<MemorySegment>> memTable = refMemoryTable.get();
        Entry<MemorySegment> oldEntry = memTable.get(entry.key());
        long newValueSize = entry.value() == null ? 0 : entry.value().byteSize();
        long oldValueSize = oldEntry == null || oldEntry.value() == null ? 0 : oldEntry.value().byteSize();
        memTable.put(entry.key(), entry);

        long usedSpaceAfterPut = usedSpace.updateAndGet(x -> x + (newValueSize - oldValueSize));

        if (usedSpaceAfterPut >= flushThresholdBytes) {
            if (!refFlushTable.get().isEmpty()) {
                log.log(Level.WARNING, "Over space.");
                throw new MemoryTableOutOfMemoryException();
            }
            flush();
        }
    }

    public void flush() {
        if (!existsSSTableManager()) {
            return;
        }

        flushWorker.submit(() -> {
            usedSpace.set(0);
            refFlushTable.set(refMemoryTable.getAndSet(createMap()));
            try {
                long id = ssTableManager.saveEntries(() -> refFlushTable.get().values().iterator());
                if (id >= 0) {
                    log.log(Level.INFO, "Flush was completed, new SSTable[id=%d]".formatted(id));
                }
            } catch (IOException e) {
                log.log(Level.WARNING, "Flush was failed", e);
            } finally {
                refFlushTable.set(createMap());
            }
        });
    }

    public void close() {
        if (!flushWorker.isShutdown()) {
            flush();
        }
        flushWorker.close();
    }

    private boolean existsSSTableManager() {
        return ssTableManager != null;
    }

    private static ConcurrentNavigableMap<MemorySegment, Entry<MemorySegment>> createMap() {
        return new ConcurrentSkipListMap<>(MemorySegmentUtils::compareMemorySegments);
    }
}
