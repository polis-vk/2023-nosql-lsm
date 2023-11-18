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
import java.util.concurrent.atomic.AtomicReference;

public class InMemoryDaoImpl implements InMemoryDao<MemorySegment, Entry<MemorySegment>> {

    private static final int MAX_MEMTABLES = 2;
    private final AtomicReference<List<Memtable>> memtables;
    private final long flushThresholdBytes;
    private final OutMemoryDao<MemorySegment, Entry<MemorySegment>> outMemoryDao;
    private final ExecutorService executorService = Executors.newSingleThreadExecutor();

    public InMemoryDaoImpl(
            final long flushThresholdBytes,
            final OutMemoryDao<MemorySegment, Entry<MemorySegment>> outMemoryDao
    ) {
        this.flushThresholdBytes = flushThresholdBytes;
        final List<Memtable> list = new ArrayList<>();
        list.add(new SkipListMemtable());
        this.memtables = new AtomicReference<>(list);
        this.outMemoryDao = outMemoryDao;
    }

    @Override
    public List<Iterator<Entry<MemorySegment>>> get(final MemorySegment from, final MemorySegment to) {
        final List<Iterator<Entry<MemorySegment>>> iterators = new ArrayList<>();
        for (final Memtable memtable : memtables.get()) {
            iterators.add(memtable.get(from, to));
        }
        return iterators;
    }

    @Override
    public Entry<MemorySegment> get(final MemorySegment key) {
        for (final Memtable memtable : memtables.get()) {
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
            final List<Memtable> currentMemtables = memtables.get();
            final Memtable memtable = currentMemtables.get(0);
            if (memtable.tryOpen()) {
                try (memtable) {
                    if (memtable.size() < flushThresholdBytes) {
                        memtable.upsert(entry);
                        return;
                    }
                }
            }
            executorService.execute(() -> tryFlush(true));
        }
    }

    @Override
    public void flush() {
        executorService.execute(() -> tryFlush(false));
    }

    private void tryFlush(final boolean isFull) {
        final List<Memtable> currentMemtables = memtables.get();
        final Memtable memtable = currentMemtables.get(0);
        if (isFull && (memtable.size() < flushThresholdBytes)) {
            return;
        }
        if (currentMemtables.size() == MAX_MEMTABLES) {
            if (isFull) {
                throw new TooManyUpsertsException("to many upserts.");
            }
            return;
        }
        final List<Memtable> newMemtables = new ArrayList<>(currentMemtables.size() + 1);
        newMemtables.add(new SkipListMemtable());
        newMemtables.addAll(currentMemtables);
        if (memtables.compareAndSet(currentMemtables, newMemtables)) {
           flush(memtable);
        }
    }

    private void flush(final Memtable memtable) {
        memtable.kill();
        while (true) {
            if (memtable.writers() == 0) {
                try {
                    outMemoryDao.flush(memtable);
                    while (true) {
                        final List<Memtable> prevList = memtables.get();
                        final List<Memtable> newList = new ArrayList<>(prevList);
                        newList.remove(memtable);
                        if (memtables.compareAndSet(prevList, newList)) {
                            break;
                        }
                    }
                } catch (final IOException e) {
                    throw new UncheckedIOException(e);
                }
                break;
            }
        }
    }

    @Override
    public void close() {
        executorService.close();
        flush(memtables.get().getFirst());
        memtables.get().clear();
    }

}
