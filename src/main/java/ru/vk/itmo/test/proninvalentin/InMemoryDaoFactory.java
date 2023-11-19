package ru.vk.itmo.test.proninvalentin;

import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;
import ru.vk.itmo.proninvalentin.DaoFacade;
import ru.vk.itmo.test.DaoFactory;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.charset.StandardCharsets;

@DaoFactory(stage = 3, week = 2)
public class InMemoryDaoFactory implements DaoFactory.Factory<MemorySegment, Entry<MemorySegment>> {

    @Override
    public Dao<MemorySegment, Entry<MemorySegment>> createDao() {
        return new DaoFacade();
    }

    @Override
    public String toString(MemorySegment memorySegment) {
        return memorySegment == null
                ? null
                : new String(memorySegment.toArray(ValueLayout.JAVA_BYTE), StandardCharsets.UTF_8);
    }

    @Override
    public MemorySegment fromString(String data) {
        return data == null
                ? null
                : MemorySegment.ofArray(data.getBytes(StandardCharsets.UTF_8));
    }

    @Override
    public Entry<MemorySegment> fromBaseEntry(Entry<MemorySegment> baseEntry) {
        return baseEntry;
    }
}
