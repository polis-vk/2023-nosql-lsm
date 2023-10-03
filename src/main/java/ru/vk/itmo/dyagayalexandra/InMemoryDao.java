package ru.vk.itmo.dyagayalexandra;

import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;

import java.lang.foreign.MemorySegment;

public class InMemoryDao extends AbstractDao implements Dao<MemorySegment, Entry<MemorySegment>> {

    @Override
    public Entry<MemorySegment> get(MemorySegment key) {
        return storage.get(key);
    }
}
