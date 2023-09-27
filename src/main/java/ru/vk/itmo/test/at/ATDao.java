package ru.vk.itmo.test.at;

import java.lang.foreign.MemorySegment;
import java.util.Comparator;
import java.util.Iterator;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

import ru.vk.itmo.BaseEntry;
import ru.vk.itmo.Dao;

/**
 * @author andrey.timofeev
 * @date 22.09.2023
 */
public class ATDao implements Dao<MemorySegment, BaseEntry<MemorySegment>> {
    NavigableMap<MemorySegment, BaseEntry<MemorySegment>> cache = new ConcurrentSkipListMap<>(Comparator.comparing(MemorySegment::asByteBuffer));

    @Override
    public Iterator<BaseEntry<MemorySegment>> get(MemorySegment from, MemorySegment to) {
        if (from == null) {
            if (to == null) {
                return cache.values().iterator();
            }
            return cache.headMap(to).values().iterator();
        }
        return cache.tailMap(from).headMap(to).values().iterator();
    }

    @Override
    public BaseEntry<MemorySegment> get(MemorySegment key) {
        return cache.get(key);
    }

    @Override
    public void upsert(BaseEntry<MemorySegment> entry) {
        cache.put(entry.key(), entry);
    }
}
