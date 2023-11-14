package ru.vk.itmo.test.osipovdaniil;

import ru.vk.itmo.Config;
import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;
import ru.vk.itmo.osipovdaniil.InMemoryDao;
import ru.vk.itmo.test.DaoFactory;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.charset.StandardCharsets;

@DaoFactory(stage = 2)
public class MyFactory implements DaoFactory.Factory<MemorySegment, Entry<MemorySegment>> {

    @Override
    public Dao<MemorySegment, Entry<MemorySegment>> createDao() {
        return new InMemoryDao();
    }

    @Override
    public Dao<MemorySegment, Entry<MemorySegment>> createDao(final Config config) {
        return new InMemoryDao(config);
    }

    @Override
    public String toString(final MemorySegment memorySegment) {
        if (memorySegment == null) {
            return null;
        }
        return new String(memorySegment.toArray(ValueLayout.JAVA_BYTE), StandardCharsets.UTF_8);
    }

    @Override
    public MemorySegment fromString(final String data) {
        if (data == null) {
            return null;
        }
        return MemorySegment.ofArray(data.getBytes(StandardCharsets.UTF_8));
    }

    @Override
    public Entry<MemorySegment> fromBaseEntry(final Entry<MemorySegment> baseEntry) {
        return baseEntry;
    }
}
