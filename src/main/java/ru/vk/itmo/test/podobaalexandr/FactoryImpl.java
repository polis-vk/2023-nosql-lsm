package ru.vk.itmo.test.podobaalexandr;

import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;
import ru.vk.itmo.podobaalexandr.InMemoryDaoImpl;
import ru.vk.itmo.test.DaoFactory;

import java.lang.foreign.MemorySegment;
import java.nio.charset.StandardCharsets;

@DaoFactory
public class FactoryImpl implements DaoFactory.Factory<MemorySegment, Entry<MemorySegment>> {
    @Override
    public Dao<MemorySegment, Entry<MemorySegment>> createDao() {
        return new InMemoryDaoImpl();
    }

    @Override
    public String toString(MemorySegment memorySegment) {
        return new String(memorySegment.asByteBuffer().array(), StandardCharsets.UTF_8);
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
