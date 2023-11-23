package ru.vk.itmo.smirnovdmitrii.inmemory;

import ru.vk.itmo.Entry;
import ru.vk.itmo.smirnovdmitrii.outofmemory.OutMemoryDao;
import ru.vk.itmo.smirnovdmitrii.util.exceptions.TooManyUpsertsException;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.foreign.MemorySegment;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

public class InMemoryDaoImpl implements InMemoryDao<MemorySegment, Entry<MemorySegment>> {

    private static final int MAX_MEMTABLES = 2;
    private final AtomicBoolean isFlushing = new AtomicBoolean(false);
    private List<Memtable> memtables;
    private final long flushThresholdBytes;
    private final OutMemoryDao<MemorySegment, Entry<MemorySegment>> outMemoryDao;
    private final ExecutorService executorService = Executors.newSingleThreadExecutor();

    public InMemoryDaoImpl(
            final long flushThresholdBytes,
            final OutMemoryDao<MemorySegment, Entry<MemorySegment>> outMemoryDao
    ) {
        this.flushThresholdBytes = flushThresholdBytes;
        this.memtables = Collections.singletonList(newMemtable());
        this.outMemoryDao = outMemoryDao;
    }

    @Override
    public List<Iterator<Entry<MemorySegment>>> get(final MemorySegment from, final MemorySegment to) {
        final List<Iterator<Entry<MemorySegment>>> iterators = new ArrayList<>();
        for (final Memtable memtable : memtables) {
            iterators.add(memtable.get(from, to));
        }
        return iterators;
    }

    @Override
    public Entry<MemorySegment> get(final MemorySegment key) {
        for (final Memtable memtable : memtables) {
            final Entry<MemorySegment> result = memtable.get(key);
            if (result != null) {
                return result;
            }
        }
        return null;
    }

    @Override
    public void upsert(final Entry<MemorySegment> entry) {
        while (true) {
            final List<Memtable> currentMemtables = memtables;
            final Memtable memtable = currentMemtables.getFirst();
            if (memtable.upsertLock().tryLock()) {
                try {
                    if (memtable.size() < flushThresholdBytes) {
                        memtable.upsert(entry);
                        break;
                    }
                } finally {
                    memtable.upsertLock().unlock();
                }
                if (isFlushing.compareAndSet(false, true)) {
                    executorService.execute(this::forceFlush);
                } else {
                    if (currentMemtables.size() == MAX_MEMTABLES) {
                        throw new TooManyUpsertsException("out of memory.");
                    }
                }
            }
            // Try again. We get SSTable that will be just replaced.
        }
    }

    @Override
    public void flush() {
        if (isFlushing.compareAndSet(false, true)) {
            executorService.execute(this::forceFlush);
        }
    }

    private void forceFlush() {
        try {
            final Memtable memtable = memtables.get(0);
            // Creating new memory table.
            memtables = List.of(newMemtable(), memtable);
            // Waiting until all upserts finished and flushing it to disk.
            memtable.flushLock().lock();
            try {
                outMemoryDao.flush(memtable);
                memtables = Collections.singletonList(memtables.get(0));
            } catch (final IOException e) {
                throw new UncheckedIOException(e);
            } finally {
                memtable.flushLock().unlock();
            }
        } finally {
            isFlushing.set(false);
        }
    }

    private static Memtable newMemtable() {
        return new SkipListMemtable();
    }

    /**
     * Flushing memtable on disk.
     */
    @Override
    public void close() {
        executorService.close();
        forceFlush();
        memtables = null;
    }

}
