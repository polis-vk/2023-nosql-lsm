package ru.vk.itmo.test.grunskiialexey;

import ru.vk.itmo.BaseEntry;
import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;
import ru.vk.itmo.grunskiialexey.MemorySegmentDao;
import ru.vk.itmo.test.DaoFactory;

import java.lang.foreign.MemorySegment;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

@DaoFactory
public class MemorySegmentDaoFactory implements DaoFactory.Factory<MemorySegment, Entry<MemorySegment>> {
    private final ConcurrentMap<String, byte[]> fromStringDict = new ConcurrentHashMap<>();

    @Override
    public Dao<MemorySegment, Entry<MemorySegment>> createDao() {
        return new MemorySegmentDao();
    }

    @Override
    public String toString(MemorySegment memorySegment) {
        // I use .asByteBuffer() because some properties like byte[] array will be linked and not copied
        // Also array() - just giving a link to the byte[] array inside ByteBuffer
        return memorySegment == null ? null : new String(memorySegment.asByteBuffer().array(), StandardCharsets.UTF_8);
    }

    @Override
    public MemorySegment fromString(String data) {
        if (data == null) {
            return null;
        }

        fromStringDict.computeIfAbsent(data, s -> s.getBytes(StandardCharsets.UTF_8));
        return MemorySegment.ofArray(fromStringDict.get(data));
    }

    @Override
    public Entry<MemorySegment> fromBaseEntry(Entry<MemorySegment> baseEntry) {
        return new BaseEntry<>(
                baseEntry.key(),
                baseEntry.value()
        );
    }

}
