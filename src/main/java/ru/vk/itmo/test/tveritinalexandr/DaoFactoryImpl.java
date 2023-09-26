package ru.vk.itmo.test.tveritinalexandr;

import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;
import ru.vk.itmo.test.DaoFactory;
import ru.vk.itmo.tveritinalexandr.InMemoryDaoImpl;

import java.lang.foreign.MemorySegment;
import java.nio.charset.StandardCharsets;

@DaoFactory
public class DaoFactoryImpl implements DaoFactory.Factory<MemorySegment, Entry<MemorySegment>> {
    @Override
    public Dao<MemorySegment, Entry<MemorySegment>> createDao() {
        return new InMemoryDaoImpl();
    }

    @Override
    public String toString(MemorySegment memorySegment) {
        return memorySegment == null ? null : new String(memorySegment.asByteBuffer().array(), StandardCharsets.UTF_8);
    }

    @Override
    public MemorySegment fromString(String data) {
        return data == null ? null : MemorySegment.ofArray(data.getBytes(StandardCharsets.UTF_8));
    }

    @Override
    public Entry<MemorySegment> fromBaseEntry(Entry<MemorySegment> baseEntry) {
        return baseEntry;
    }
}
