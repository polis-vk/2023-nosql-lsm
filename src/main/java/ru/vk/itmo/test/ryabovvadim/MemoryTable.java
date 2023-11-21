package ru.vk.itmo.test.ryabovvadim;

import ru.vk.itmo.Entry;
import ru.vk.itmo.test.ryabovvadim.exception.MemoryTableOutOfMemoryException;
import ru.vk.itmo.test.ryabovvadim.sstable.SSTableManager;
import ru.vk.itmo.test.ryabovvadim.utils.MemorySegmentUtils;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.foreign.MemorySegment;
import java.util.Iterator;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

public class MemoryTable {
    private final AtomicReference<ConcurrentNavigableMap<MemorySegment, Entry<MemorySegment>>> refMemoryTable =
            new AtomicReference<>(new ConcurrentSkipListMap<>(MemorySegmentUtils::compareMemorySegments));
    private final ExecutorService flushWorker = Executors.newSingleThreadExecutor();
    private final SSTableManager ssTableManager;
    private final long flushThresholdBytes;
    private final AtomicLong usedSpace = new AtomicLong();
    private final AtomicBoolean isFlushed = new AtomicBoolean();

    public MemoryTable(SSTableManager ssTableManager, long flushThresholdBytes) {
        this.ssTableManager = ssTableManager;
        this.flushThresholdBytes = flushThresholdBytes;
    }

    public Entry<MemorySegment> get(MemorySegment key) {
        return refMemoryTable.get().get(key);
    }

    public Iterator<Entry<MemorySegment>> get(MemorySegment from, MemorySegment to) {
        if (from == null && to == null) {
            return refMemoryTable.get().values().iterator();
        } else if (from == null) {
            return refMemoryTable.get().headMap(to).values().iterator();
        } else if (to == null) {
            return refMemoryTable.get().tailMap(from).values().iterator();
        }

        return refMemoryTable.get().subMap(from, to).values().iterator();
    }

    public void upsert(Entry<MemorySegment> entry) {
        refMemoryTable.get().put(entry.key(), entry);

        if (usedSpace.get() >= flushThresholdBytes) {
            if (isFlushed.get()) {
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
            isFlushed.set(true);
            ConcurrentNavigableMap<MemorySegment, Entry<MemorySegment>> memTable = refMemoryTable.get();
            refMemoryTable.set(new ConcurrentSkipListMap<>(MemorySegmentUtils::compareMemorySegments));

            try {
                ssTableManager.saveEntries(() -> memTable.values().iterator());
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            } finally {
                isFlushed.set(false);
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
}
