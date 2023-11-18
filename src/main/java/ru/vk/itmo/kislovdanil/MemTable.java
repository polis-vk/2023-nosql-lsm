package ru.vk.itmo.kislovdanil;

import ru.vk.itmo.Entry;
import ru.vk.itmo.kislovdanil.exceptions.DBException;

import java.lang.foreign.MemorySegment;
import java.util.Comparator;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicInteger;

/* Basically, ConcurrentSkipList with counter of threads that putting value in it.
    Necessary for preventing data loss while flushing.
 */
public class MemTable {
    public ConcurrentSkipListMap<MemorySegment, Entry<MemorySegment>> storage;
    private final AtomicInteger puttingThreadsCount = new AtomicInteger(0);

    public MemTable(Comparator<MemorySegment> comparator) {
        this.storage = new ConcurrentSkipListMap<>(comparator);
    }

    public Entry<MemorySegment> put(Entry<MemorySegment> entry) {
        puttingThreadsCount.incrementAndGet();
        Entry<MemorySegment> wasBefore = storage.put(entry.key(), entry);
        puttingThreadsCount.decrementAndGet();
        return wasBefore;
    }

    public void waitPuttingThreads() {
        try {
            while (puttingThreadsCount.get() > 0) {
                Thread.sleep(10);
            }
        } catch (InterruptedException e) {
            throw new DBException(e);
        }
    }
}
