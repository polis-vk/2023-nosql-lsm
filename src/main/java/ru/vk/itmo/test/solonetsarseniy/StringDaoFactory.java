package ru.vk.itmo.test.solonetsarseniy;

import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;
import ru.vk.itmo.solonetsarseniy.InMemoryDao;
import ru.vk.itmo.solonetsarseniy.transformer.StringMemorySegmentTransformer;
import ru.vk.itmo.solonetsarseniy.transformer.Transformer;
import ru.vk.itmo.test.DaoFactory;

import java.lang.foreign.MemorySegment;

@DaoFactory
public class StringDaoFactory implements DaoFactory.Factory<MemorySegment, Entry<MemorySegment>> {
    private final Transformer<String, MemorySegment> transformer = new StringMemorySegmentTransformer();

    @Override
    public Dao<MemorySegment, Entry<MemorySegment>> createDao() {
        return new InMemoryDao();
    }

    @Override
    public String toString(MemorySegment memorySegment) {
        return transformer.toTarget(memorySegment);
    }

    @Override
    public MemorySegment fromString(String data) {
        if (data == null) return null;
        return transformer.toSource(data);
    }

    @Override
    public Entry<MemorySegment> fromBaseEntry(Entry<MemorySegment> baseEntry) {
        return baseEntry;
    }
}
