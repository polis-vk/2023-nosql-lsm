package ru.vk.itmo.solonetsarseniy;

import ru.vk.itmo.Config;
import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;
import ru.vk.itmo.solonetsarseniy.exception.DaoException;
import ru.vk.itmo.solonetsarseniy.exception.DaoExceptions;
import ru.vk.itmo.solonetsarseniy.helpers.DataStorageManager;
import ru.vk.itmo.solonetsarseniy.helpers.DatabaseBuilder;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.util.Iterator;
import java.util.concurrent.ConcurrentSkipListMap;

public class InMemoryDao implements Dao<MemorySegment, Entry<MemorySegment>> {
    private static final DatabaseBuilder databaseBuilder = new DatabaseBuilder();
    private final ConcurrentSkipListMap<MemorySegment, Entry<MemorySegment>> database = databaseBuilder.build();
    DataStorageManager dataStorageManager;

    public InMemoryDao(Config config) {
        this.dataStorageManager = new DataStorageManager(config);
    }

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

        Entry<MemorySegment> entry = database.get(key);
        if (entry != null) {
            return entry;
        }
        return dataStorageManager.get(key);
    }

    @Override
    public void upsert(Entry<MemorySegment> entry) {
        MemorySegment key = entry.key();
        if (key == null) {
            DaoException.throwException(DaoExceptions.NULL_KEY_PUT);
        }
        database.put(key, entry);
    }

    @Override
    public void flush() throws IOException {
        dataStorageManager.flush(database);
    }

    @Override
    public void close() throws IOException {
        flush();
    }
}
