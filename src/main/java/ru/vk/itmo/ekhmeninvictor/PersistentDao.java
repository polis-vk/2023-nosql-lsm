package ru.vk.itmo.ekhmeninvictor;

import ru.vk.itmo.Config;
import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.util.Collections;
import java.util.Iterator;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class PersistentDao implements Dao<MemorySegment, Entry<MemorySegment>> {
    private final NavigableMap<MemorySegment, Entry<MemorySegment>> cache;
    private final SSTable table;

    public PersistentDao(Config config) throws IOException {
        this.cache = new ConcurrentSkipListMap<>(new MemorySegmentComparator());
        this.table = new SSTable(config);
    }

    @Override
    public Entry<MemorySegment> get(MemorySegment key) {
        Entry<MemorySegment> fromCache = cache.get(key);
        if (fromCache == null) {
            return table.get(key);
        }
        return fromCache;
    }

    @Override
    public Iterator<Entry<MemorySegment>> get(MemorySegment from, MemorySegment to) {
        if (cache.isEmpty()) {
            return Collections.emptyIterator();
        }
        return cache.subMap(
                from == null ? cache.firstKey() : from, true,
                to == null ? cache.lastKey() : to, to == null
        ).values().iterator();
    }

    @Override
    public void upsert(Entry<MemorySegment> entry) {
        cache.put(entry.key(), entry);
    }

    @Override
    public void close() throws IOException {
        table.save(cache);
    }
}
