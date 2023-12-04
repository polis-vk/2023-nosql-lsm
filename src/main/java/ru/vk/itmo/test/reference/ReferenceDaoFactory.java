package ru.vk.itmo.test.reference;

import ru.vk.itmo.Config;
import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;
import ru.vk.itmo.reference.ReferenceDao;
import ru.vk.itmo.test.DaoFactory;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.charset.StandardCharsets;

/**
 * Instantiates {@link ReferenceDao}.
 *
 * @author incubos
 */
@DaoFactory(stage = 5)
public class ReferenceDaoFactory implements DaoFactory.Factory<MemorySegment, Entry<MemorySegment>> {
    @Override
    public Dao<MemorySegment, Entry<MemorySegment>> createDao(final Config config) throws IOException {
        return new ReferenceDao(config);
    }

    @Override
    public String toString(final MemorySegment memorySegment) {
        if (memorySegment == null) {
            return null;
        }

        final byte[] array = memorySegment.toArray(ValueLayout.JAVA_BYTE);
        return new String(array, StandardCharsets.UTF_8);
    }

    @Override
    public MemorySegment fromString(final String data) {
        return data == null
                ? null
                : MemorySegment.ofArray(
                data.getBytes(
                        StandardCharsets.UTF_8));
    }

    @Override
    public Entry<MemorySegment> fromBaseEntry(final Entry<MemorySegment> baseEntry) {
        return baseEntry;
    }
}
