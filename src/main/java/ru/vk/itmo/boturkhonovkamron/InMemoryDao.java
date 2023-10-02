package ru.vk.itmo.boturkhonovkamron;

import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;

import java.lang.foreign.MemorySegment;
import java.util.Iterator;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * Реализация интерфейса Dao для работы с объектами типа MemorySegment в памяти.
 *
 * @author Kamron Boturkhonov
 * @since 2023.09.26
 */
public class InMemoryDao implements Dao<MemorySegment, Entry<MemorySegment>> {

    final ConcurrentNavigableMap<MemorySegment, Entry<MemorySegment>> data;

    public InMemoryDao() {
        data = new ConcurrentSkipListMap<>(new MemorySegmentComparator());
    }

    @Override
    public Entry<MemorySegment> get(final MemorySegment key) {
        return data.get(key);
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
        data.merge(entry.key(), entry, (oldValue, newValue) -> newValue);
    }
}
