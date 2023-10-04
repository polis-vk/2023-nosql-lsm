package ru.vk.itmo.boturkhonovkamron;

import ru.vk.itmo.Config;
import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;
import ru.vk.itmo.boturkhonovkamron.io.SSTable;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.util.Iterator;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * Implementation of Dao interface for in-memory storage of MemorySegment objects.
 *
 * @author Kamron Boturkhonov
 * @since 2023.09.26
 */
public class InMemoryDao implements Dao<MemorySegment, Entry<MemorySegment>> {

    private final ConcurrentNavigableMap<MemorySegment, Entry<MemorySegment>> data;

    private SSTable ssTable;

    public InMemoryDao() {
        data = new ConcurrentSkipListMap<>(new MemorySegmentComparator());
    }

    public InMemoryDao(final Config config) throws IOException {
        this();
        this.ssTable = new SSTable(config);
    }

    @Override
    public Entry<MemorySegment> get(final MemorySegment key) {
        return getSafe(key);
    }

    private Entry<MemorySegment> getSafe(final MemorySegment key) {
        if (data.containsKey(key)) {
            return data.get(key);
        }
        if (ssTable != null) {
            return ssTable.getEntity(key);
        }
        return null;
    }

    @Override
    public Iterator<Entry<MemorySegment>> get(final MemorySegment from, final MemorySegment to) {
        final ConcurrentNavigableMap<MemorySegment, Entry<MemorySegment>> subMap;
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
        data.put(entry.key(), entry);
    }

    @Override
    public void flush() throws IOException {
        if (data.size() > 0) {
            ssTable.saveData(data);
            data.clear();
        }
    }

    @Override
    public void close() throws IOException {
        if (ssTable != null) {
            flush();
        }
    }
}
