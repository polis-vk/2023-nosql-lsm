package ru.vk.itmo.test.grunskiialexey;

import ru.vk.itmo.BaseEntry;
import ru.vk.itmo.Config;
import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;
import ru.vk.itmo.grunskiialexey.MemorySegmentDao;
import ru.vk.itmo.test.DaoFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

@DaoFactory(stage = 2)
public class MemorySegmentDaoFactory implements DaoFactory.Factory<MemorySegment, Entry<MemorySegment>> {

    @Override
    public Dao<MemorySegment, Entry<MemorySegment>> createDao() {
        return new MemorySegmentDao();
    }

    @Override
    public Dao<MemorySegment, Entry<MemorySegment>> createDao(Config config) throws IOException {
        Path filePath = Paths.get(config.basePath().toString(), "file");
        Files.createDirectories(filePath.getParent());
        if (!Files.exists(filePath)) {
            return new MemorySegmentDao(List.of(), filePath);
        }

        try (FileInputStream ch = new FileInputStream(filePath.toFile())) {
            List<Entry<MemorySegment>> list = new ArrayList<>();
            byte[] array = new byte[4];
            while (ch.read(array) > 0) {
                int lengthKey = ByteBuffer.wrap(array).getInt();
                byte[] key = new byte[lengthKey];
                ch.read(key);
                ch.read(array);
                int lengthValue = ByteBuffer.wrap(array).getInt();
                byte[] value = new byte[lengthValue];
                ch.read(value);
                list.add(new BaseEntry<>(
                        MemorySegment.ofArray(key),
                        MemorySegment.ofArray(value)
                ));
            }

            return new MemorySegmentDao(
                    list,
                    filePath
            );
        }
    }

    @Override
    public String toString(MemorySegment memorySegment) {
        return memorySegment == null
                ? null
                : new String(memorySegment.toArray(ValueLayout.JAVA_BYTE), StandardCharsets.UTF_8);
    }

    @Override
    public MemorySegment fromString(String data) {
        return data == null ? null : MemorySegment.ofArray(data.getBytes(StandardCharsets.UTF_8));
    }

    @Override
    public Entry<MemorySegment> fromBaseEntry(Entry<MemorySegment> baseEntry) {
        return new BaseEntry<>(
                baseEntry.key(),
                baseEntry.value()
        );
    }
}
