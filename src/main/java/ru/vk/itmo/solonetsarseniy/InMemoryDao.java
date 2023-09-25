package ru.vk.itmo.solonetsarseniy;

import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;

import java.lang.foreign.MemorySegment;
import java.util.Iterator;
import java.util.concurrent.ConcurrentSkipListMap;

public class InMemoryDao implements Dao<MemorySegment, Entry<MemorySegment>> {
    private static final DatabaseBuilder databaseBuilder = new DatabaseBuilder();
    private final ConcurrentSkipListMap<MemorySegment, Entry<MemorySegment>> database = databaseBuilder.build();

    @Override
    public Iterator<Entry<MemorySegment>> get(MemorySegment from, MemorySegment to) {
        if (from == null && to == null) {
            return database.values()
                .iterator();
        }
        if (from == null) {
            return database.headMap(to)
                .values()
                .iterator();
        }
        if (to == null) {
            return database.tailMap(from)
                .values()
                .iterator();
        }

        return database.subMap(from, to)
            .values()
            .iterator();
    }

    @Override
    public Entry<MemorySegment> get(MemorySegment key) {
        if (key == null) DaoException.NULL_KEY_GET.throwException();
        return database.get(key);
    }

    @Override
    public void upsert(Entry<MemorySegment> entry) {
        MemorySegment key = entry.key();
        if (key == null) DaoException.NULL_KEY_PUT.throwException();
        database.put(key, entry);
    }
}
