package ru.vk.itmo.test.ekhmeninvictor;

import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;
import ru.vk.itmo.test.DaoFactory;

import java.lang.foreign.MemorySegment;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.concurrent.ConcurrentSkipListMap;

public class InMemoryDao implements Dao<MemorySegment, Entry<MemorySegment>> {

    private final DaoFactory.Factory<MemorySegment, Entry<MemorySegment>> myFactory = new DaoFactoryImpl();

    private final ConcurrentSkipListMap<MemorySegment, Entry<MemorySegment>> cache =
            new ConcurrentSkipListMap<>(Comparator.comparing(myFactory::toString));

    @Override
    public Entry<MemorySegment> get(MemorySegment key) {
        return cache.get(key);
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
}
