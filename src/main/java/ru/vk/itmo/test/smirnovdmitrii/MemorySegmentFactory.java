package ru.vk.itmo.test.smirnovdmitrii;

import java.lang.foreign.MemorySegment;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;
import ru.vk.itmo.smirnovdmitrii.InMemoryDao;
import ru.vk.itmo.test.DaoFactory;

@DaoFactory
public class MemorySegmentFactory implements DaoFactory.Factory<MemorySegment, Entry<MemorySegment>> {

    final Map<String, MemorySegment> memorySegmentMap = new ConcurrentHashMap<>();

    @Override
    public Dao<MemorySegment, Entry<MemorySegment>> createDao() {
        return new InMemoryDao();
    }

    @Override
    public String toString(final MemorySegment memorySegment) {
        if (memorySegment == null) {
            return null;
        }
        memorySegment.getUtf8String(0);
        return Charset.defaultCharset().decode(memorySegment.asByteBuffer()).toString();
    }

    @Override
    public MemorySegment fromString(final String data) {
        if (data == null) {
            return null;
        }

        return memorySegmentMap.computeIfAbsent(data,  k -> MemorySegment.ofArray(k.getBytes(StandardCharsets.UTF_8)));
    }

    @Override
    public Entry<MemorySegment> fromBaseEntry(final Entry<MemorySegment> baseEntry) {
        return baseEntry;
    }
}
