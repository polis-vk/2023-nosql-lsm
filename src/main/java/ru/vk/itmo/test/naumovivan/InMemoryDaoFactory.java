package ru.vk.itmo.test.naumovivan;

import ru.vk.itmo.Config;
import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;
import ru.vk.itmo.naumovivan.InMemoryDao;
import ru.vk.itmo.test.DaoFactory;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.charset.StandardCharsets;

@DaoFactory(stage = 3)
public class InMemoryDaoFactory implements DaoFactory.Factory<MemorySegment, Entry<MemorySegment>> {
    @Override
    public Dao<MemorySegment, Entry<MemorySegment>> createDao(final Config config) throws IOException {
        return new InMemoryDao(config);
    }

    @Override
    public String toString(final MemorySegment memorySegment) {
        return new String(memorySegment.toArray(ValueLayout.JAVA_BYTE), StandardCharsets.UTF_8);
    }

    @Override
    public MemorySegment fromString(final String data) {
        return data == null ? null : MemorySegment.ofArray(data.getBytes(StandardCharsets.UTF_8)).asReadOnly();
    }

    @Override
    public Entry<MemorySegment> fromBaseEntry(final Entry<MemorySegment> baseEntry) {
        return baseEntry;
    }
}
