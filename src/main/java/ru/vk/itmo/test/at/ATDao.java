package ru.vk.itmo.test.at;

import java.lang.foreign.MemorySegment;
import java.util.Comparator;
import java.util.Iterator;
import java.util.NavigableMap;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentSkipListMap;

import ru.vk.itmo.BaseEntry;
import ru.vk.itmo.Dao;

/**
 * @author andrey.timofeev
 * @date 22.09.2023
 */
public class ATDao implements Dao<MemorySegment, BaseEntry<MemorySegment>> {
    NavigableMap<MemorySegment, BaseEntry<MemorySegment>> cache = new ConcurrentSkipListMap<>(Comparator.comparing(MemorySegment::asByteBuffer));

    public Iterator<BaseEntry<MemorySegment>> get(MemorySegment from, MemorySegment to) {
        SortedMap<MemorySegment, BaseEntry<MemorySegment>> map = from == null
                ? cache
                : cache.tailMap(from);
        if (to != null) {
            map = map.headMap(to);
        }
        return map.values().iterator();
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
