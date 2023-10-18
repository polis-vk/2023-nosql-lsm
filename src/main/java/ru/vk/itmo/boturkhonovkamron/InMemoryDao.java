package ru.vk.itmo.boturkhonovkamron;

import ru.vk.itmo.Config;
import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;
import ru.vk.itmo.boturkhonovkamron.persistence.PersistentDao;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.util.Iterator;
import java.util.NavigableMap;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * Implementation of Dao interface for in-memory storage of MemorySegment objects.
 *
 * @author Kamron Boturkhonov
 * @since 2023.09.26
 */
public class InMemoryDao implements Dao<MemorySegment, Entry<MemorySegment>> {

    private final NavigableMap<MemorySegment, Entry<MemorySegment>> data;

    private final NavigableMap<MemorySegment, Entry<MemorySegment>> deletedData;

    private PersistentDao persistentDao;

    public InMemoryDao() {
        data = new ConcurrentSkipListMap<>(MemorySegmentComparator.COMPARATOR);
        deletedData = new ConcurrentSkipListMap<>(MemorySegmentComparator.COMPARATOR);
    }

    public InMemoryDao(final Config config) throws IOException {
        this();
        this.persistentDao = new PersistentDao(config, data, deletedData);
    }

    @Override
    public Entry<MemorySegment> get(final MemorySegment key) {
        return getSafe(key);
    }

    private Entry<MemorySegment> getSafe(final MemorySegment key) {
        if (data.containsKey(key)) {
            return data.get(key);
        }
        if (deletedData.containsKey(key)) {
            return null;
        }
        if (persistentDao != null) {
            return persistentDao.getEntity(key);
        }
        return null;
    }

    @Override
    public Iterator<Entry<MemorySegment>> get(final MemorySegment from, final MemorySegment to) {
        if (persistentDao != null) {
            return persistentDao.getIterator(from, to);
        }
        final SortedMap<MemorySegment, Entry<MemorySegment>> subMap;
        if (from == null && to == null) {
            subMap = data;
        } else if (from == null) {
            subMap = data.headMap(to, false);
        } else if (to == null) {
            subMap = data.tailMap(from, true);
        } else {
            subMap = data.subMap(from, to);
        }
        return subMap.values().iterator();
    }

    @Override
    public void upsert(final Entry<MemorySegment> entry) {
        if (entry.value() == null) {
            data.remove(entry.key());
            deletedData.put(entry.key(), entry);
        } else {
            data.put(entry.key(), entry);
            deletedData.remove(entry.key());
        }
    }

    @Override
    public void flush() throws IOException {
        data.putAll(deletedData);
        if (data.size() > 0) {
            persistentDao.saveData(data);
        }
        data.clear();
        deletedData.clear();
    }

    @Override
    public void close() throws IOException {
        if (persistentDao != null) {
            flush();
        }
    }
}
