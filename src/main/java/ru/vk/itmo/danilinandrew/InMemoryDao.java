package ru.vk.itmo.danilinandrew;

import ru.vk.itmo.BaseEntry;
import ru.vk.itmo.Config;
import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Iterator;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class InMemoryDao implements Dao<MemorySegment, Entry<MemorySegment>> {
    private final ConcurrentNavigableMap<MemorySegment, Entry<MemorySegment>> data =
            new ConcurrentSkipListMap<>(new MemorySegmentComparator());
    private final Path ssTablePath;
    private final Arena readArena = Arena.ofConfined();
    private final MemorySegment mappedMemorySegment;

    public InMemoryDao(Config config) {
        ssTablePath = config.basePath().resolve("data.txt");

        MemorySegment tempMemorySegment;
        try (FileChannel fileChannel = FileChannel.open(ssTablePath)) {
            long size = Files.size(ssTablePath);
            tempMemorySegment = fileChannel.map(FileChannel.MapMode.READ_ONLY, 0, size, readArena);
        } catch (IOException e) {
            tempMemorySegment = null;
        }

        mappedMemorySegment = tempMemorySegment;
    }

    @Override
    public Entry<MemorySegment> get(MemorySegment key) {
        Entry<MemorySegment> value = data.get(key);

        if (value != null) {
            return value;
        }

        if (mappedMemorySegment == null) {
            return null;
        }

        long offset = 0;
        while (offset < mappedMemorySegment.byteSize()) {
            long keySize = mappedMemorySegment.get(ValueLayout.JAVA_LONG_UNALIGNED, offset);
            offset += Long.BYTES;

            if (keySize != key.byteSize()) {
                offset += keySize;
                long valueSize = mappedMemorySegment.get(ValueLayout.JAVA_LONG_UNALIGNED, offset);
                offset += Long.BYTES + valueSize;
                continue;
            }

            MemorySegment readKey = mappedMemorySegment.asSlice(offset, keySize);
            offset += keySize;
            if (key.mismatch(readKey) == -1) {
                long valueSize = mappedMemorySegment.get(ValueLayout.JAVA_LONG_UNALIGNED, offset);
                offset += Long.BYTES;
                MemorySegment readValue = mappedMemorySegment.asSlice(offset, valueSize);

                return new BaseEntry<>(key, readValue);
            }

        }

        return null;
    }

    @Override
    public Iterator<Entry<MemorySegment>> get(MemorySegment from, MemorySegment to) {
        if (from == null && to == null) {
            return data.values().iterator();
        }

        if (from == null) {
            return data.headMap(to).values().iterator();
        }

        if (to == null) {
            return data.tailMap(from).values().iterator();
        }

        return data.subMap(from, to).values().iterator();
    }

    @Override
    public void upsert(Entry<MemorySegment> entry) {
        if (entry == null) {
            return;
        }

        data.put(entry.key(), entry);
    }

    @Override
    public void close() throws IOException {
        readArena.close();

        try (Arena writeArena = Arena.ofConfined()) {
            long size = 0;

            for (Entry<MemorySegment> value : data.values()) {
                size += value.key().byteSize() + value.value().byteSize();
            }

            size += 2L * Long.BYTES * size;

            try (FileChannel fileChannel = FileChannel.open(
                    ssTablePath,
                    StandardOpenOption.WRITE,
                    StandardOpenOption.TRUNCATE_EXISTING,
                    StandardOpenOption.CREATE,
                    StandardOpenOption.READ
            )) {
                MemorySegment page = fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, size, writeArena);

                long offset = 0;

                for (Entry<MemorySegment> value : data.values()) {
                    page.set(ValueLayout.JAVA_LONG_UNALIGNED, offset, value.key().byteSize());
                    offset += Long.BYTES;

                    page.asSlice(offset).copyFrom(value.key());
                    offset += value.key().byteSize();

                    page.set(ValueLayout.JAVA_LONG_UNALIGNED, offset, value.value().byteSize());
                    offset += Long.BYTES;

                    page.asSlice(offset).copyFrom(value.value());
                    offset += value.value().byteSize();
                }
            }
        }
    }
}
