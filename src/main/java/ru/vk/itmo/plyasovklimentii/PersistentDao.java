package ru.vk.itmo.plyasovklimentii;

import ru.vk.itmo.Config;
import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.util.Iterator;
import java.util.concurrent.ConcurrentSkipListMap;

public class PersistentDao implements Dao<MemorySegment, Entry<MemorySegment>> {

    private final SSTable ssTable;
    private final ConcurrentSkipListMap<MemorySegment, Entry<MemorySegment>> storage;

    public PersistentDao(Config config) throws IOException {
        this.ssTable = new SSTable(config);
        this.storage = new ConcurrentSkipListMap<>(new MemoryComparator());
    }

    @Override
    public Entry<MemorySegment> get(MemorySegment key) {
        return storage.get(key) == null ? ssTable.get(key) : storage.get(key);
    }

    @Override
    public Iterator<Entry<MemorySegment>> get(MemorySegment from, MemorySegment to) {
        if (from == null && to == null) {
            return storage.sequencedValues().iterator();
        }
        if (from == null) {
            return storage.headMap(to, false).sequencedValues().iterator();
        }
        if (to == null) {
            return storage.tailMap(from, true).sequencedValues().iterator();
        } else {
            return storage.subMap(from,true, to, false).sequencedValues().iterator();
        }
    }

    @Override
    public void upsert(Entry<MemorySegment> entry) {
        storage.put(entry.key(), entry);
    }

    @Override
    public void flush() throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void close() throws IOException {
        if (!storage.isEmpty()) ssTable.save(storage);
    }
}
