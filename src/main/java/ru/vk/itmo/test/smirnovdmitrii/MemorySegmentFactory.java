package ru.vk.itmo.test.smirnovdmitrii;

import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;
import ru.vk.itmo.smirnovdmitrii.InMemoryDao;
import ru.vk.itmo.test.DaoFactory;

import java.lang.foreign.MemorySegment;
import java.nio.charset.Charset;

@DaoFactory
public class MemorySegmentFactory implements DaoFactory.Factory<MemorySegment, Entry<MemorySegment>> {

    @Override
    public Dao<MemorySegment, Entry<MemorySegment>> createDao() {
        return new InMemoryDao();
    }

    @Override
    public String toString(final MemorySegment memorySegment) {
        return memorySegment == null ? null : Charset.defaultCharset().decode(memorySegment.asByteBuffer()).toString();
    }

    @Override
    public MemorySegment fromString(final String data) {
        return data == null ? null : MemorySegment.ofArray(data.getBytes(Charset.defaultCharset()));
    }

    @Override
    public Entry<MemorySegment> fromBaseEntry(final Entry<MemorySegment> baseEntry) {
        return baseEntry;
    }
}
