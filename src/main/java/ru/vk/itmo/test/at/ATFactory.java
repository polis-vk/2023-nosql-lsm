package ru.vk.itmo.test.at;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.charset.StandardCharsets;

import ru.vk.itmo.BaseEntry;
import ru.vk.itmo.Config;
import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;
import ru.vk.itmo.at.ATDao;
import ru.vk.itmo.test.DaoFactory;

@DaoFactory(stage = 2)
public class ATFactory implements DaoFactory.Factory<MemorySegment, BaseEntry<MemorySegment>> {

    @Override
    public Dao createDao(Config config) throws IOException {
        return new ATDao(config);
    }

    @Override
    public String toString(MemorySegment s) {
        return s == null ? null : new String(s.toArray(ValueLayout.OfByte.JAVA_BYTE), StandardCharsets.UTF_8);
    }

    @Override
    public MemorySegment fromString(String data) {
        return data == null ? null : MemorySegment.ofArray(data.getBytes(StandardCharsets.UTF_8));
    }

    @Override
    public BaseEntry<MemorySegment> fromBaseEntry(Entry<MemorySegment> baseEntry) {
        return (BaseEntry<MemorySegment>) baseEntry;
    }
}
