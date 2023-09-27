package ru.vk.itmo.test.viktorkorotkikh;

import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;
import ru.vk.itmo.test.DaoFactory;
import ru.vk.itmo.viktorkorotkikh.InMemoryDaoImpl;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.charset.StandardCharsets;

@DaoFactory
public class FactoryImpl implements DaoFactory.Factory<MemorySegment, Entry<MemorySegment>> {
    @Override
    public Dao<MemorySegment, Entry<MemorySegment>> createDao() {
        return new InMemoryDaoImpl();
    }

    @Override
    public String toString(MemorySegment memorySegment) {
        return memorySegment == null ? null : new String(memorySegment.toArray(ValueLayout.JAVA_BYTE), StandardCharsets.UTF_8);
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
