package ru.vk.itmo.kislovdanil;

import ru.vk.itmo.Entry;

import java.lang.foreign.MemorySegment;
import java.util.Comparator;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/* Basically, ConcurrentSkipList with ReadWriteLock to identify the moment when all inserts are done.
    Necessary for preventing data loss while flushing.
 */
public class MemTable {
    public ConcurrentSkipListMap<MemorySegment, Entry<MemorySegment>> storage;
    public final ReadWriteLock lock = new ReentrantReadWriteLock();

    public MemTable(Comparator<MemorySegment> comparator) {
        this.storage = new ConcurrentSkipListMap<>(comparator);
    }

    public Entry<MemorySegment> put(Entry<MemorySegment> entry) {
        lock.readLock().lock();
        try {
            return storage.put(entry.key(), entry);
        } finally {
            lock.readLock().unlock();
        }
    }
}
