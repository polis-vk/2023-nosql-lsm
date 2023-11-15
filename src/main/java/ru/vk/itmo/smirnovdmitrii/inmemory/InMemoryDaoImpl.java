package ru.vk.itmo.smirnovdmitrii.inmemory;

import ru.vk.itmo.Entry;
import ru.vk.itmo.smirnovdmitrii.outofmemory.OutMemoryDao;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

public class InMemoryDaoImpl implements InMemoryDao<MemorySegment, Entry<MemorySegment>> {

    private static final int MAX_MEMTABLES = 2;
    private final AtomicReference<List<Memtable>> memtables;
    private final long flushThresholdBytes;
    private final OutMemoryDao<MemorySegment, Entry<MemorySegment>> outMemoryDao;

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
        for (final Memtable memtable: memtables.get()) {
            iterators.add(memtable.get(from, to));
        }
        return iterators;
    }

    @Override
    public Entry<MemorySegment> get(final MemorySegment key) {
        for (final Memtable memtable: memtables.get()) {
            final Entry<MemorySegment> result = memtable.get(key);
            if (result != null) {
                return result;
            }
        }
        return null;
    }

    @Override
    public void upsert(final Entry<MemorySegment> entry) throws IOException {
        while (true) {
            final List<Memtable> currentMemtables = memtables.get();
            final Memtable memtable = currentMemtables.get(0);
            if (memtable.size() >= flushThresholdBytes) {
                if (currentMemtables.size() == MAX_MEMTABLES) {
                    throw new OutOfMemoryError("TOO MANY UPSERTS!!!!");
                }
                tryUpdate(currentMemtables, memtable);
            } else {
                memtable.upsert(entry);
                break;
            }
        }
    }

    @Override
    public void flush() throws IOException {
        final List<Memtable> currentMemtables = memtables.get();
        if (currentMemtables.size() == MAX_MEMTABLES) {
            return;
        }
        final Memtable memtable = currentMemtables.get(0);
        tryUpdate(currentMemtables, memtable);
    }

    private void tryUpdate(final List<Memtable> currentMemtables, final Memtable memtable) throws IOException {
        final List<Memtable> newMemtables = new ArrayList<>(currentMemtables.size() + 1);
        newMemtables.add(new SkipListMemtable());
        newMemtables.addAll(currentMemtables);
        if (memtables.compareAndSet(currentMemtables, newMemtables)) {
            outMemoryDao.flush(memtable);
            removeLast();
        }
    }

    private void removeLast() {
        while (true) {
            final List<Memtable> prevList = memtables.get();
            final List<Memtable> newList = new ArrayList<>(prevList);
            newList.removeLast();
            if (memtables.compareAndSet(prevList, newList)) {
                return;
            }
        }
    }

    @Override
    public void close() {
        memtables.get().clear();
    }

}
