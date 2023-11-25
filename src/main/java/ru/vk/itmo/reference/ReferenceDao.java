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
    public ReferenceDao(final Config config) {
    }

    @Override
    public Iterator<Entry<MemorySegment>> get(final MemorySegment from, final MemorySegment to) {
        throw new UnsupportedOperationException("Not implemented (yet)!");
    }

    @Override
    public void upsert(final Entry<MemorySegment> entry) {
        throw new UnsupportedOperationException("Not implemented (yet)!");
    }
}
