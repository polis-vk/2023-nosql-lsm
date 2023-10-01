package ru.vk.itmo.test.abramovilya;

import ru.vk.itmo.Config;
import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;
import ru.vk.itmo.abramovilya.InMemoryDao;
import ru.vk.itmo.test.DaoFactory;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;

@DaoFactory(stage = 2)
public class DaoFactoryImpl implements DaoFactory.Factory<MemorySegment, Entry<MemorySegment>> {
    @Override
    public Dao<MemorySegment, Entry<MemorySegment>> createDao(Config config) throws IOException {
        return new InMemoryDao(config);
    }

    @Override
    public Dao<MemorySegment, Entry<MemorySegment>> createDao() {
        return new InMemoryDao();
    }

    @Override
    public String toString(MemorySegment memorySegment) {
        if (memorySegment == null) return null;
        return new String(memorySegment.toArray(ValueLayout.JAVA_BYTE), StandardCharsets.UTF_8);
    }

    @Override
    public MemorySegment fromString(String data) {
        if (data == null) return null;
        return MemorySegment.ofArray(data.getBytes(StandardCharsets.UTF_8));
    }

    @Override
    public Entry<MemorySegment> fromBaseEntry(Entry<MemorySegment> baseEntry) {
        return baseEntry;
    }
}
