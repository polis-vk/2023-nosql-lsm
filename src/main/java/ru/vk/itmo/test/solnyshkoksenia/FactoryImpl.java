package ru.vk.itmo.test.solnyshkoksenia;

import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;
import ru.vk.itmo.solnyshkoksenia.DaoImpl;
import ru.vk.itmo.test.DaoFactory;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

@DaoFactory
public class FactoryImpl implements DaoFactory.Factory<MemorySegment, Entry<MemorySegment>> {
    @Override
    public Dao<MemorySegment, Entry<MemorySegment>> createDao() {
        return new DaoImpl();
    }

    @Override
    public String toString(MemorySegment memorySegment) {
        return memorySegment == null ? null : String.valueOf(memorySegment.toArray(ValueLayout.JAVA_CHAR));
    }

    @Override
    public MemorySegment fromString(String data) {
        return data == null ? MemorySegment.NULL : MemorySegment.ofArray(data.toCharArray());
    }

    @Override
    public Entry<MemorySegment> fromBaseEntry(Entry<MemorySegment> baseEntry) {
        return baseEntry;
    }
}
