package ru.vk.itmo.solonetsarseniy;

import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;
import ru.vk.itmo.solonetsarseniy.exception.DaoException;
import ru.vk.itmo.solonetsarseniy.exception.DaoExceptions;
import ru.vk.itmo.solonetsarseniy.helpers.DatabaseBuilder;

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
        if (key == null) {
            DaoException.throwException(DaoExceptions.NULL_KEY_GET);
        }
        return database.get(key);
    }

    @Override
    public void upsert(Entry<MemorySegment> entry) {
        MemorySegment key = entry.key();
        if (key == null) {
            DaoException.throwException(DaoExceptions.NULL_KEY_PUT);
        }
        database.put(key, entry);
    }
}
