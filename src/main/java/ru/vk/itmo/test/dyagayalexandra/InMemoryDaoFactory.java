package ru.vk.itmo.test.dyagayalexandra;

import ru.vk.itmo.BaseEntry;
import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;
import ru.vk.itmo.dyagayalexandra.InMemoryDao;
import ru.vk.itmo.test.DaoFactory;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.charset.StandardCharsets;

@DaoFactory
public class InMemoryDaoFactory implements DaoFactory.Factory<MemorySegment, Entry<MemorySegment>> {
    @Override
    public Dao<MemorySegment, Entry<MemorySegment>> createDao() {
        return new InMemoryDao();
    }

    @Override
    public String toString(MemorySegment memorySegment) {
        if (memorySegment == null) {
            return null;
        }

        long baseAddress = memorySegment.address();
        long byteSize = memorySegment.byteSize();
        byte[] byteArray = new byte[(int) byteSize];

        for (int i = 0; i < byteSize; i++) {
            byteArray[i] = memorySegment.get(ValueLayout.JAVA_BYTE, baseAddress + i);
        }

        return new String(byteArray, StandardCharsets.UTF_8);
    }

    @Override
    public MemorySegment fromString(String data) {
        return data == null ? null : MemorySegment.ofArray(data.getBytes(StandardCharsets.UTF_8));
    }

    @Override
    public BaseEntry<MemorySegment> fromBaseEntry(Entry<MemorySegment> baseEntry) {
        return baseEntry == null ? null : new BaseEntry<>(baseEntry.key(), baseEntry.value());
    }
}
