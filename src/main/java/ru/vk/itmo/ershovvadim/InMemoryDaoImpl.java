package ru.vk.itmo.ershovvadim;

import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;

import java.lang.foreign.MemorySegment;

public class InMemoryDaoImpl extends AbstractMemoryDao implements Dao<MemorySegment, Entry<MemorySegment>> {

    @Override
    public Entry<MemorySegment> get(MemorySegment key) {
        return db.get(key);
    }

}
