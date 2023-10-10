package ru.vk.itmo.test.cheshevandrey;

import ru.vk.itmo.Config;
import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;
import ru.vk.itmo.cheshevandrey.InMemoryDao;
import ru.vk.itmo.test.DaoFactory;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

import static java.nio.charset.StandardCharsets.UTF_8;

@DaoFactory(stage = 2)
public class InMemoryFactory implements DaoFactory.Factory<MemorySegment, Entry<MemorySegment>> {

    @Override
    public Dao<MemorySegment, Entry<MemorySegment>> createDao(Config config) throws IOException {
        return new InMemoryDao(config);
    }

    @Override
    public String toString(MemorySegment memorySegment) {
        if (memorySegment == null) {
            return null;
        }

        return new String(memorySegment.toArray(ValueLayout.JAVA_BYTE), UTF_8);
    }

    @Override
    public MemorySegment fromString(String data) {
        return (data == null) ? null : MemorySegment.ofArray(data.getBytes(UTF_8));
    }

    @Override
    public Entry<MemorySegment> fromBaseEntry(Entry<MemorySegment> baseEntry) {
        return baseEntry;
    }
}
