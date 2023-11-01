package ru.vk.itmo.test.ershovvadim;

import ru.vk.itmo.Config;
import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;
import ru.vk.itmo.ershovvadim.InMemoryDaoImpl;
import ru.vk.itmo.ershovvadim.hw3.PersistenceManyFilesDao;
import ru.vk.itmo.test.DaoFactory;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.charset.StandardCharsets;

@DaoFactory(stage = 3)
public class PersistenceManyFilesDaoFactoryImpl implements DaoFactory.Factory<MemorySegment, Entry<MemorySegment>> {

    @Override
    public Dao<MemorySegment, Entry<MemorySegment>> createDao() {
        return new InMemoryDaoImpl();
    }

    @Override
    public Dao<MemorySegment, Entry<MemorySegment>> createDao(Config config) throws IOException {
        return new PersistenceManyFilesDao(config);
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
