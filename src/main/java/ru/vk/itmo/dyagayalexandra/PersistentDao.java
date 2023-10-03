package ru.vk.itmo.dyagayalexandra;

import ru.vk.itmo.BaseEntry;
import ru.vk.itmo.Config;
import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.file.Files;
import java.nio.file.Path;

public class PersistentDao extends AbstractDao implements Dao<MemorySegment, Entry<MemorySegment>> {

    private final Path path;

    public PersistentDao(Config config) {
        path = config.basePath().resolve("storage.txt");
    }

    @Override
    public Entry<MemorySegment> get(MemorySegment key) {
        try {
            return getEntry(key);
        } catch (IOException ex) {
            return null;
        }
    }

    @Override
    public void flush() throws IOException {
        if (!Files.exists(path)) {
            Files.createFile(path);
        }

        try (FileOutputStream fos = new FileOutputStream(String.valueOf(path));
             BufferedOutputStream writer = new BufferedOutputStream(fos)) {
            for (var entry : storage.values()) {
                writer.write((byte) entry.key().byteSize());
                writer.write(entry.key().toArray(ValueLayout.JAVA_BYTE));
                writer.write((byte) entry.value().byteSize());
                writer.write(entry.value().toArray(ValueLayout.JAVA_BYTE));
            }
        }
    }

    private Entry<MemorySegment> getEntry(MemorySegment key) throws IOException {
        if (storage.containsKey(key)) {
            return storage.get(key);
        }

        MemorySegmentComparator comparator = new MemorySegmentComparator();

        try (FileInputStream fis = new FileInputStream(String.valueOf(path));
             BufferedInputStream reader = new BufferedInputStream(fis)) {

            while (reader.available() != 0) {
                int keyLength = reader.read();
                byte[] keyBytes = reader.readNBytes(keyLength);
                int valueLength = reader.read();
                byte[] valueBytes = reader.readNBytes(valueLength);

                if (comparator.compare(MemorySegment.ofArray(keyBytes), key) == 0) {
                    return new BaseEntry<>(key, MemorySegment.ofArray(valueBytes));
                }
            }
        }

        return null;
    }
}
