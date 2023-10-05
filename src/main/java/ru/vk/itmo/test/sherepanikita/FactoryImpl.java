package ru.vk.itmo.test.sherepanikita;

import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;
import ru.vk.itmo.sherepanikita.InMemoryDao;
import ru.vk.itmo.test.DaoFactory;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.charset.StandardCharsets;

@DaoFactory
public class FactoryImpl implements DaoFactory.Factory<MemorySegment, Entry<MemorySegment>> {

    @Override
    public Dao<MemorySegment, Entry<MemorySegment>> createDao() {
        return new InMemoryDao();
    }

    @Override
    public String toString(MemorySegment memorySegment) {
        if (memorySegment == null) {
            return null;
        } else {
            return new String(
                    memorySegment.toArray(ValueLayout.JAVA_BYTE),
                    StandardCharsets.UTF_8
            );
        }
    }

    @Override
    public MemorySegment fromString(String data) {
        if (data == null) {
            return null;
        } else {
            return MemorySegment.ofArray(data.getBytes(StandardCharsets.UTF_8));
        }
    }

    @Override
    public Entry<MemorySegment> fromBaseEntry(Entry<MemorySegment> baseEntry) {
        return baseEntry;
    }
}
