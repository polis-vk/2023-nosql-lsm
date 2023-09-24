package ru.vk.itmo.test.svistukhinandrey;

import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;
import ru.vk.itmo.svistukhinandrey.InMemoryDao;
import ru.vk.itmo.test.DaoFactory;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

@DaoFactory
public class DaoFactoryImpl implements DaoFactory.Factory<MemorySegment, Entry<MemorySegment>> {
    @Override
    public String toString(MemorySegment memorySegment) {
        return Utils.transform(memorySegment);
    }

    @Override
    public MemorySegment fromString(String data) {
        if (data == null) return null;
        return MemorySegment.ofArray(data.toCharArray());
    }

    @Override
    public Entry<MemorySegment> fromBaseEntry(Entry<MemorySegment> baseEntry) {
        return baseEntry;
    }

    @Override
    public Dao<MemorySegment, Entry<MemorySegment>> createDao() {
        return new InMemoryDao();
    }
}
