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

    private final ConcurrentSkipListMap<MemorySegment, Entry<MemorySegment>> inMemoryDao =
            new ConcurrentSkipListMap<>(Comparator.comparing(myFactory::toString));

    @Override
    public Entry<MemorySegment> get(MemorySegment key) {
        return inMemoryDao.get(key);
    }

    @Override
    public Iterator<Entry<MemorySegment>> get(MemorySegment from, MemorySegment to) {

        if (inMemoryDao.isEmpty()) {
            return Collections.emptyIterator();
        }
        return inMemoryDao.subMap(
                from == null ? inMemoryDao.firstKey() : from, true,
                to == null ? inMemoryDao.lastKey() : to, to == null // игнорируем правую границу, если она задана и не игнорируем, если необходимо идти до конца коллекции
        ).values().iterator();
    }


    @Override
    public void upsert(Entry<MemorySegment> entry) {
        inMemoryDao.put(entry.key(), entry);
    }
}
