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
    private final ConcurrentMap<String, MemorySegment> fromStringDict = new ConcurrentHashMap<>();

    @Override
    public Dao<MemorySegment, Entry<MemorySegment>> createDao() {
        return new MemorySegmentDao();
    }

    @Override
    public String toString(MemorySegment memorySegment) {
        if (memorySegment == null) {
            return null;
        }

        // I use .asByteBuffer() because some properties like byte[] array will be linked and not copied
        // Also array() - just giving a link to the byte[] array inside ByteBuffer
        return new String(memorySegment.asByteBuffer().array(), StandardCharsets.UTF_8);
    }

    @Override
    public MemorySegment fromString(String data) {
        if (data == null) {
            return null;
        }

        fromStringDict.computeIfAbsent(data, s -> MemorySegment.ofArray(s.getBytes(StandardCharsets.UTF_8)));
        return fromStringDict.get(data);
    }

    @Override
    public Entry<MemorySegment> fromBaseEntry(Entry<MemorySegment> baseEntry) {
        return new BaseEntry<>(
                baseEntry.key(),
                baseEntry.value()
        );
    }

}
