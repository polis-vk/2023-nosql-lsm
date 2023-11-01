package ru.vk.itmo.test.kovalevigor;

import ru.vk.itmo.Config;
import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;
import ru.vk.itmo.kovalevigor.DaoImpl;
import ru.vk.itmo.test.DaoFactory;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

@DaoFactory(stage = 2)
public class DaoFactoryImpl implements DaoFactory.Factory<MemorySegment, Entry<MemorySegment>> {

    public static final Charset CHARSET = StandardCharsets.UTF_8;

    @Override
    public Dao<MemorySegment, Entry<MemorySegment>> createDao() {
        return null;
    }

    @Override
    public Dao<MemorySegment, Entry<MemorySegment>> createDao(Config config) throws IOException {
        return new DaoImpl(config);
    }

    @Override
    public String toString(final MemorySegment memorySegment) {
        return memorySegment == null ? null : new String(memorySegment.toArray(ValueLayout.JAVA_BYTE), CHARSET);
    }

    @Override
    public MemorySegment fromString(final String data) {
        return data == null ? null : MemorySegment.ofArray(data.getBytes(CHARSET));
    }

    @Override
    public Entry<MemorySegment> fromBaseEntry(Entry<MemorySegment> baseEntry) {
        return baseEntry;
    }
}
