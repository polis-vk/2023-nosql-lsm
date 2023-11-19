package ru.vk.itmo.mozzhevilovdanil;

import ru.vk.itmo.Config;
import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;
import ru.vk.itmo.mozzhevilovdanil.iterators.DatabaseIterator;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.util.Iterator;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class MozzhevilovDao implements Dao<MemorySegment, Entry<MemorySegment>> {

    private final NavigableMap<MemorySegment, Entry<MemorySegment>> storage =
            new ConcurrentSkipListMap<>(DatabaseUtils.comparator);
    private final TablesManager tablesManager;

    public MozzhevilovDao(Config config) throws IOException {
        this.tablesManager = new TablesManager(config);
    }

    @Override
    public Iterator<Entry<MemorySegment>> get(MemorySegment from, MemorySegment to) {
        return new DatabaseIterator(getMap(from, to), tablesManager.get(from, to));
    }

    private Iterator<Entry<MemorySegment>> getMap(MemorySegment from, MemorySegment to) {
        if (from == null && to == null) {
            return storage.values().iterator();
        }
        if (from == null) {
            return storage.headMap(to).values().iterator();
        }
        if (to == null) {
            return storage.tailMap(from).values().iterator();
        }
        return storage.subMap(from, to).values().iterator();
    }

    @Override
    public void upsert(Entry<MemorySegment> entry) {
        storage.put(entry.key(), entry);
    }

    @Override
    public Entry<MemorySegment> get(MemorySegment key) {
        Entry<MemorySegment> entry = storage.get(key);
        if (entry != null) {
            return entry.value() == null ? null : entry;
        }
        return tablesManager.get(key);
    }

    @Override
    public void close() throws IOException {
        tablesManager.store(storage, false);
    }

    @Override
    public void compact() throws IOException {
        tablesManager.compact(storage);
    }
}
