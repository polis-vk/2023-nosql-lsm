package ru.vk.itmo.smirnovdmitrii.inmemory;

import ru.vk.itmo.Entry;
import ru.vk.itmo.smirnovdmitrii.outofmemory.OutMemoryDao;
import ru.vk.itmo.smirnovdmitrii.util.exceptions.TooManyUpsertsException;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.foreign.MemorySegment;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class InMemoryDaoImpl implements InMemoryDao<MemorySegment, Entry<MemorySegment>> {

    private static final int MAX_MEMTABLES = 2;
    private List<Memtable> memtables;
    private final long flushThresholdBytes;
    private final OutMemoryDao<MemorySegment, Entry<MemorySegment>> outMemoryDao;
    private final ExecutorService executorService = Executors.newSingleThreadExecutor();

    public InMemoryDaoImpl(
            final long flushThresholdBytes,
            final OutMemoryDao<MemorySegment, Entry<MemorySegment>> outMemoryDao
    ) {
        this.flushThresholdBytes = flushThresholdBytes;
        final List<Memtable> list = new ArrayList<>();
        list.add(newMemtable());
        this.memtables = list;
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
                executorService.execute(() -> tryFlush(true));
            }
            // Try again. We get SSTable that will be just replaced.
        }
    }

    @Override
    public void flush() {
        executorService.execute(() -> tryFlush(false));
    }

    /**
     * Tries to flush memtable. Parameter {@code isFull} true, if we flush because of threshehold,
     * or false, if it is requested flush.
     */
    private void tryFlush(final boolean isFull) {
        final Memtable memtable = memtables.get(0);
        // Maybe was already flushed, while task was waiting.
        if (isFull && (memtable.size() < flushThresholdBytes)) {
            return;
        }
        // Can't create new Memtable because of max count.
        if (memtables.size() == MAX_MEMTABLES) {
            if (isFull) {
                throw new TooManyUpsertsException("to many upserts.");
            }
            return;
        }
        // Creating new memory table.
        final List<Memtable> newMemtables = new ArrayList<>();
        newMemtables.add(newMemtable());
        newMemtables.addAll(memtables);
        memtables = newMemtables;
        // Waiting until all upserts finished and flushing it to disk.
        memtable.flushLock().lock();
        try {
            outMemoryDao.flush(memtable);
            final List<Memtable> removed = new ArrayList<>(memtables);
            removed.removeLast();
            memtables = removed;
        } catch (final IOException e) {
            throw new UncheckedIOException(e);
        } finally {
            memtable.flushLock().unlock();
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
        tryFlush(false);
        memtables = null;
    }

}
