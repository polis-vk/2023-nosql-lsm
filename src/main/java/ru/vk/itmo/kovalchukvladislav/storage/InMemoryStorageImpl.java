package ru.vk.itmo.kovalchukvladislav.storage;

import ru.vk.itmo.Entry;
import ru.vk.itmo.kovalchukvladislav.SSTableMemorySegmentWriter;
import ru.vk.itmo.kovalchukvladislav.model.EntryExtractor;
import ru.vk.itmo.kovalchukvladislav.model.TableInfo;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class InMemoryStorage<D, E extends Entry<D>> {
    private final long flushThresholdBytes;
    private final EntryExtractor<D, E> extractor;
    private final SSTableMemorySegmentWriter<D, E> writer;
    private final ReadWriteLock daoChangeLock = new ReentrantReadWriteLock();
    private final ExecutorService backgroundQueue = Executors.newSingleThreadExecutor();

    // Следущие три поля меняются одновременно и атомарно с daoChangeLock
    private final AtomicLong daoSize = new AtomicLong(0);
    private volatile ConcurrentNavigableMap<D, E> dao;
    private volatile ConcurrentNavigableMap<D, E> flushingDao;
    private final EntryExtractor<D, E> extractor;
    private final SSTableMemorySegmentWriter<D, E> writer;

    public InMemoryStorage(long flushThresholdBytes,
                           EntryExtractor<D, E> extractor,
                           SSTableMemorySegmentWriter<D, E> writer) {
        this.flushThresholdBytes = flushThresholdBytes;
        this.extractor = extractor;
        this.writer = writer;
        this.dao = new ConcurrentSkipListMap<>(extractor);
    }

    public E get(D key) {
        daoChangeLock.readLock().lock();
        try {
            E entry = dao.get(key);
            if (entry == null && flushingDao != null) {
                entry = flushingDao.get(key);
            }
            return entry;
        } finally {
            daoChangeLock.readLock().unlock();
        }
    }

    public void upsert(E entry) {
        daoChangeLock.readLock().lock();
        try {
            E oldEntry = dao.put(entry.key(), entry);
            long delta = extractor.size(entry) - extractor.size(oldEntry);
            daoSize.addAndGet(delta);
        } finally {
            daoChangeLock.readLock().unlock();
        }
    }

    public List<Iterator<E>> getIterators(D from, D to) {
        daoChangeLock.readLock().lock();
        try {
            List<Iterator<E>> result = new ArrayList<>(2);
            if (dao != null) {
                result.add(getIteratorDao(dao, from, to));
            }
            if (flushingDao != null) {
                result.add(getIteratorDao(flushingDao, from, to));
            }
            return result;
        } finally {
            daoChangeLock.readLock().unlock();
        }
    }

    public void flush() {
        daoChangeLock.writeLock().lock();
        try {
            this.flushingDao = dao;
            long flushingDaoSize = daoSize.getAndSet(0);
            this.dao = new ConcurrentSkipListMap<>(extractor);
        } finally {
            daoChangeLock.writeLock().unlock();
        }
        writer.flush(flushingDao.values().iterator(), new TableInfo());

    }

    private Iterator<E> getIteratorDao(ConcurrentNavigableMap<D, E> dao, D from, D to) {
        ConcurrentNavigableMap<D, E> subMap;
        if (from == null && to == null) {
            subMap = dao;
        } else if (from == null) {
            subMap = dao.headMap(to);
        } else if (to == null) {
            subMap = dao.tailMap(from);
        } else {
            subMap = dao.subMap(from, to);
        }
        return subMap.values().iterator();
    }
}
