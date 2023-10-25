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
import java.util.Collection;
import java.util.Iterator;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class Storage implements Dao<MemorySegment, Entry<MemorySegment>> {

    protected final NavigableMap<MemorySegment, Entry<MemorySegment>> dataStorage;
    private final Path path;
    private FileInputStream fis;
    private BufferedInputStream reader;

    public Storage() {
        dataStorage = new ConcurrentSkipListMap<>(new MemorySegmentComparator());
        path = null;
    }

    public Storage(Config config) {
        dataStorage = new ConcurrentSkipListMap<>(new MemorySegmentComparator());
        path = config.basePath().resolve("storage.txt");
        try {
            fis = new FileInputStream(String.valueOf(path));
            reader = new BufferedInputStream(fis);
        } catch (IOException e) {
            throw new IllegalArgumentException("Failed to open the file.", e);
        }
    }

    @Override
    public Iterator<Entry<MemorySegment>> get(MemorySegment from, MemorySegment to) {
        Collection<Entry<MemorySegment>> values;
        if (from == null && to == null) {
            values = dataStorage.values();
        } else if (from == null) {
            values = dataStorage.headMap(to).values();
        } else if (to == null) {
            values = dataStorage.tailMap(from).values();
        } else {
            values = dataStorage.subMap(from, to).values();
        }

        return values.iterator();
    }

    @Override
    public void upsert(Entry<MemorySegment> entry) {
        dataStorage.put(entry.key(), entry);
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
            for (var entry : dataStorage.values()) {
                writer.write((byte) entry.key().byteSize());
                writer.write(entry.key().toArray(ValueLayout.JAVA_BYTE));
                writer.write((byte) entry.value().byteSize());
                writer.write(entry.value().toArray(ValueLayout.JAVA_BYTE));
            }
        }

        fis.close();
        reader.close();
    }

    private Entry<MemorySegment> getEntry(MemorySegment key) throws IOException {
        Entry<MemorySegment> entry = dataStorage.get(key);
        if (entry != null) {
            return entry;
        }

        Entry<MemorySegment> result = null;
        while (reader.available() != 0 && result == null) {
            int keyLength = reader.read();
            if (keyLength != key.byteSize()) {
                reader.skipNBytes(keyLength);
                reader.skipNBytes(reader.read());
                continue;
            }

            byte[] keyBytes = reader.readNBytes(keyLength);
            if (!key.equals(MemorySegment.ofArray(keyBytes))) {
                reader.skipNBytes(reader.read());
                continue;
            }

            int valueLength = reader.read();
            byte[] valueBytes = reader.readNBytes(valueLength);
            result = new BaseEntry<>(key, MemorySegment.ofArray(valueBytes));
        }

        return null;
    }
}
