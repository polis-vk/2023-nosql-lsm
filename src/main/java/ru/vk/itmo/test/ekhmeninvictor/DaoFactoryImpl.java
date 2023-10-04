package ru.vk.itmo.test.ekhmeninvictor;

import ru.vk.itmo.Config;
import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;
import ru.vk.itmo.ekhmeninvictor.PersistentDao;
import ru.vk.itmo.test.DaoFactory;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.charset.StandardCharsets;

@DaoFactory(stage = 2)
public class DaoFactoryImpl implements DaoFactory.Factory<MemorySegment, Entry<MemorySegment>> {
    @Override
    public Dao<MemorySegment, Entry<MemorySegment>> createDao(Config config) throws IOException {
        return new PersistentDao(config);
    }

    @Override
    public String toString(MemorySegment memorySegment) {
        if (memorySegment == null) {
            return null;
        }

        byte[] array = memorySegment.toArray(ValueLayout.JAVA_BYTE);
        return new String(array, StandardCharsets.UTF_8);
    }

    @Override
    public MemorySegment fromString(String data) {
        if (data == null) {
            return null;
        }
        return MemorySegment.ofArray(data.getBytes(StandardCharsets.UTF_8));
    }

    @Override
    public Entry<MemorySegment> fromBaseEntry(Entry<MemorySegment> baseEntry) {
        return baseEntry;
    }
}
