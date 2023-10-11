package ru.vk.itmo.svistukhinandrey;

import ru.vk.itmo.Config;
import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.util.Iterator;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class PersistentDao implements Dao<MemorySegment, Entry<MemorySegment>> {

    private final NavigableMap<MemorySegment, Entry<MemorySegment>> storage;
    private final SSTable ssTable;

    public PersistentDao(Config config) throws IOException {
        storage = new ConcurrentSkipListMap<>(new MemorySegmentComparator());
        ssTable = new SSTable(config);
    }

    @Override
    public Entry<MemorySegment> get(MemorySegment key) {
        Entry<MemorySegment> storageEntry = storage.get(key);
        if (storageEntry != null) {
            return storageEntry;
        }

        return ssTable.get(key);
    }

    @Override
    public Iterator<Entry<MemorySegment>> get(MemorySegment from, MemorySegment to) {
        if (from == null && to == null) {
            return storage.sequencedValues().iterator();
        }

        boolean isLastInclusive = false;
        MemorySegment localFrom = (from == null) ? storage.firstKey() : from;
        MemorySegment localTo = to;

        if (to == null) {
            localTo = storage.lastKey();
            isLastInclusive = true;
        }

        return storage.subMap(localFrom, true, localTo, isLastInclusive).sequencedValues().iterator();
    }

    @Override
    public synchronized void upsert(Entry<MemorySegment> entry) {
        storage.put(entry.key(), entry);
    }

    @Override
    public void flush() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void close() throws IOException {
        ssTable.save(storage.sequencedValues());
    }
}
