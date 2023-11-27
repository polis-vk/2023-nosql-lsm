package ru.vk.itmo.dalbeevgeorgii;

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

    private final Config config;
    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    private final Storage storage;

    private final NavigableMap<MemorySegment, Entry<MemorySegment>> map =
            new ConcurrentSkipListMap<>(MemorySegmentComparator::compare);

    public DaoImpl(Config config) throws IOException {
        this.config = config;
        storage = Storage.load(config);
    }

    @Override
    public Iterator<Entry<MemorySegment>> get(MemorySegment from, MemorySegment to) {
        lock.readLock().lock();
        try {
            if (from == null) {
                return storage.getIterator(MemorySegment.NULL, to, getMemoryIterator(MemorySegment.NULL, to));
            }

            return storage.getIterator(from, to, getMemoryIterator(from, to));
        } finally {
            lock.readLock().unlock();
        }
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
    public void upsert(Entry<MemorySegment> entry) {
        lock.readLock().lock();
        try {
            map.put(entry.key(), entry);
        } finally {
            lock.readLock().unlock();
        }
    }

    private Iterator<Entry<MemorySegment>> getMemoryIterator(MemorySegment from, MemorySegment to) {
        lock.readLock().lock();
        try {
            if (from == null && to == null) {
                return map.values().iterator();
            } else if (to == null) {
                return map.tailMap(from).values().iterator();
            } else if (from == null) {
                return map.headMap(to).values().iterator();
            }
            return map.subMap(from, to).values().iterator();
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public void flush() {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public void close() throws IOException {
        storage.close();
        lock.writeLock().lock();
        try {
            Storage.save(config, map.values(), storage);
        } finally {
            lock.writeLock().unlock();
        }
    }
}
