package ru.vk.itmo.test.kholoshniavadim.inmemory;

import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;
import ru.vk.itmo.test.DaoFactory;
import ru.vk.itmo.test.kholoshniavadim.utils.StringConverter;

import java.lang.foreign.MemorySegment;

@DaoFactory
public class InMemoryFactory implements DaoFactory.Factory<MemorySegment, Entry<MemorySegment>> {
    @Override
    public Dao<MemorySegment, Entry<MemorySegment>> createDao() {
        return new InMemoryDao();
    }

    @Override
    public String toString(MemorySegment memorySegment) {
        return StringConverter.toString(memorySegment);
    }

    @Override
    public MemorySegment fromString(String data) {
        return StringConverter.fromString(data);
    }

    @Override
    public Entry<MemorySegment> fromBaseEntry(Entry<MemorySegment> baseEntry) {
        return baseEntry;
    }
}
