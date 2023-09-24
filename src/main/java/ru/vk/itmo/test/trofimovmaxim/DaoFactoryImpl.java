package ru.vk.itmo.test.trofimovmaxim;

import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;
import ru.vk.itmo.test.DaoFactory;
import ru.vk.itmo.trofimovmaxim.InMemoryDao;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

@DaoFactory
public class DaoFactoryImpl implements DaoFactory.Factory<MemorySegment, Entry<MemorySegment>> {
    @Override
    public Dao<MemorySegment, Entry<MemorySegment>> createDao() {
        return new InMemoryDao();
    }

    @Override
    public String toString(MemorySegment memorySegment) {
//        System.out.println("a=" + memorySegment.getUtf8String(0));
//        return memorySegment == null ? null : memorySegment.getUtf8String(0);
        return  memorySegment == null ? null : new String(memorySegment.toArray(ValueLayout.OfChar.JAVA_CHAR));
//        return memorySegment == null ? null : memorySegment.getUtf8String(0);
    }

    @Override
    public MemorySegment fromString(String data) {
//        System.out.println(data);
//        System.out.println(toString(MemorySegment.ofArray(data.toCharArray())));
        return data == null ? null : MemorySegment.ofArray(data.toCharArray());
    }

    @Override
    public Entry<MemorySegment> fromBaseEntry(Entry<MemorySegment> baseEntry) {
        return baseEntry;
    }
}
