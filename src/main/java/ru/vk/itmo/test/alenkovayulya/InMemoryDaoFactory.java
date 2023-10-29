package ru.vk.itmo.test.alenkovayulya;

import ru.vk.itmo.Config;
import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;
import ru.vk.itmo.alenkovayulya.AlenkovaDao;
import ru.vk.itmo.test.DaoFactory;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.file.Files;
import java.nio.file.Path;

import static java.nio.charset.StandardCharsets.UTF_8;

@DaoFactory(stage = 4)
public class InMemoryDaoFactory implements DaoFactory.Factory<MemorySegment, Entry<MemorySegment>> {

    @Override
    public Dao<MemorySegment, Entry<MemorySegment>> createDao() throws IOException {
        Path tmp = Files.createTempDirectory("dao");
        return new AlenkovaDao(new Config(tmp));
    }

    @Override
    public Dao<MemorySegment, Entry<MemorySegment>> createDao(Config config) throws IOException {
        return new AlenkovaDao(config);
    }

    @Override
    public String toString(MemorySegment memorySegment) {
        return memorySegment == null ? null : new String(memorySegment.toArray(ValueLayout.JAVA_BYTE), UTF_8);
    }

    @Override
    public MemorySegment fromString(String data) {

        return data == null ? null : MemorySegment.ofArray(data.getBytes(UTF_8));
    }

    @Override
    public Entry<MemorySegment> fromBaseEntry(Entry<MemorySegment> baseEntry) {
        return baseEntry;
    }
}
