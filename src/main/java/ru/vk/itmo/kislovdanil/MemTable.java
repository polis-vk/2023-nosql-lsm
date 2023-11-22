package ru.vk.itmo.kislovdanil;

import ru.vk.itmo.Entry;

import java.lang.foreign.MemorySegment;
import java.util.Comparator;
import java.util.concurrent.ConcurrentSkipListMap;

/* Basically, ConcurrentSkipList with counter of threads that putting value in it.
    Necessary for preventing data loss while flushing.
 */
public class MemTable {
    public ConcurrentSkipListMap<MemorySegment, Entry<MemorySegment>> storage;

    public MemTable(Comparator<MemorySegment> comparator) {
        this.storage = new ConcurrentSkipListMap<>(comparator);
    }

    public Entry<MemorySegment> put(Entry<MemorySegment> entry) {
        return storage.put(entry.key(), entry);
    }
}
