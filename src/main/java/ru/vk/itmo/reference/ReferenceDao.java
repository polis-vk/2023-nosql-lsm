package ru.vk.itmo.reference;

import ru.vk.itmo.Config;
import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;

import java.lang.foreign.MemorySegment;
import java.util.Iterator;

/**
 * Reference implementation of {@link Dao}.
 *
 * @author incubos
 */
public class ReferenceDao implements Dao<MemorySegment, Entry<MemorySegment>> {
    private final MemTable memTable = new MemTable();

    public ReferenceDao(final Config config) {
    }

    @Override
    public Iterator<Entry<MemorySegment>> get(
            final MemorySegment from,
            final MemorySegment to) {
        return memTable.get(from, to);
    }

    @Override
    public Entry<MemorySegment> get(final MemorySegment key) {
        return memTable.get(key);
    }

    @Override
    public void upsert(final Entry<MemorySegment> entry) {
        memTable.upsert(entry);
    }
}
