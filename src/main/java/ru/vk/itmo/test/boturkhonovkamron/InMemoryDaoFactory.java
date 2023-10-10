package ru.vk.itmo.test.boturkhonovkamron;

import ru.vk.itmo.Config;
import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;
import ru.vk.itmo.boturkhonovkamron.InMemoryDao;
import ru.vk.itmo.test.DaoFactory;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.charset.StandardCharsets;

/**
 * Realization of DaoFactory.
 *
 * @author Kamron Boturkhonov
 * @since 2023.09.26
 */
@DaoFactory(stage = 2)
public class InMemoryDaoFactory implements DaoFactory.Factory<MemorySegment, Entry<MemorySegment>> {

    @Override
    public Dao<MemorySegment, Entry<MemorySegment>> createDao() {
        return new InMemoryDao();
    }

    @Override
    public Dao<MemorySegment, Entry<MemorySegment>> createDao(final Config config) throws IOException {
        return new InMemoryDao(config);
    }

    @Override
    public String toString(final MemorySegment memorySegment) {
        return memorySegment == null ? null
                : new String(memorySegment.toArray(ValueLayout.JAVA_BYTE), StandardCharsets.UTF_8);
    }

    @Override
    public MemorySegment fromString(final String data) {
        return data == null ? null : MemorySegment.ofArray(data.getBytes(StandardCharsets.UTF_8));
    }

    @Override
    public Entry<MemorySegment> fromBaseEntry(final Entry<MemorySegment> baseEntry) {
        return baseEntry;
    }
}
