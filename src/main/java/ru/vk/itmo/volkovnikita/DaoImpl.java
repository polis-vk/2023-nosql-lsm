package ru.vk.itmo.volkovnikita;

import ru.vk.itmo.Config;
import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.util.Iterator;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class DaoImpl implements Dao<MemorySegment, Entry<MemorySegment>> {

    private final NavigableMap<MemorySegment, Entry<MemorySegment>> memorySegmentEntries =
            new ConcurrentSkipListMap<>(MemorySegmentComparator::compare);
    private final Store store;
    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    public DaoImpl(Config config) throws IOException {
        this.store = new Store(config);
    }

    @Override
    public Entry<MemorySegment> get(MemorySegment key) {
        lock.readLock().lock();
        try {
            Iterator<Entry<MemorySegment>> iterator = get(key, null);
            if (!iterator.hasNext()) {
                return null;
            }
            Entry<MemorySegment> next = iterator.next();
            if (MemorySegmentComparator.compare(key, next.key()) == 0) {
                return next;
            }
            return null;
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public Iterator<Entry<MemorySegment>> get(MemorySegment from, MemorySegment to) {
        lock.readLock().lock();
        try {
            if (from == null) {
                return store.getIterator(MemorySegment.NULL, to, getMemoryIterator(MemorySegment.NULL, to));
            }
            return store.getIterator(from, to, getMemoryIterator(from, to));
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public void upsert(Entry<MemorySegment> entry) {
        lock.readLock().lock();
        try {
            memorySegmentEntries.put(entry.key(), entry);
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public Iterator<Entry<MemorySegment>> all() {
        return memorySegmentEntries.values().iterator();
    }

    @Override
    public void close() throws IOException {
        store.close();
        lock.writeLock().lock();
        try {
            store.saveMemoryData(memorySegmentEntries);
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public void flush() throws IOException {
        throw new UnsupportedOperationException("Not implement");
    }

    private Iterator<Entry<MemorySegment>> getMemoryIterator(MemorySegment from, MemorySegment to) {
        lock.readLock().lock();
        try {
            if (from == null && to == null) {
                return memorySegmentEntries.values().iterator();
            } else if (to == null) {
                return memorySegmentEntries.tailMap(from).values().iterator();
            } else if (from == null) {
                return memorySegmentEntries.headMap(to).values().iterator();
            }
            return memorySegmentEntries.subMap(from, to).values().iterator();
        } finally {
            lock.readLock().unlock();
        }
    }
}
