package ru.vk.itmo.smirnovdmitrii;

import ru.vk.itmo.Config;
import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;
import ru.vk.itmo.OutMemoryDao;
import ru.vk.itmo.smirnovdmitrii.util.MemorySegmentComparator;

import java.lang.foreign.MemorySegment;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.Map;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class InMemoryDao implements Dao<MemorySegment, Entry<MemorySegment>> {

    private static final Config DEFAULT_CONFIG = new Config(Path.of(""));
    private final NavigableMap<MemorySegment, Entry<MemorySegment>> storage =
            new ConcurrentSkipListMap<>(new MemorySegmentComparator());
    private final OutMemoryDao<MemorySegment, Entry<MemorySegment>> outMemoryDao;

    public InMemoryDao() {
        this(DEFAULT_CONFIG);
    }

    public InMemoryDao(final Config config) {
        final Path basePath = config.basePath();
        outMemoryDao = new FileDao(basePath);
    }

    @Override
    public Iterator<Entry<MemorySegment>> get(final MemorySegment from, final MemorySegment to) {
        final Map<MemorySegment, Entry<MemorySegment>> map;
        if (from == null && to == null) {
            map = storage;
        } else if (from == null) {
            map = storage.headMap(to);
        } else if (to == null) {
            map = storage.tailMap(from);
        } else {
            map = storage.subMap(from, to);
        }
        return map.values().iterator();
    }

    @Override
    public Entry<MemorySegment> get(final MemorySegment key) {
        final Entry<MemorySegment> result = key == null ? null : storage.get(key);
        return result == null ? outMemoryDao.get(key) : result;
    }

    @Override
    public void upsert(final Entry<MemorySegment> entry) {
        storage.put(entry.key(), entry);
    }

    @Override
    public synchronized void close() {
        outMemoryDao.save(storage);
    }

}
