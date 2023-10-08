package ru.vk.itmo.pashchenkoalexandr;

import ru.vk.itmo.BaseEntry;
import ru.vk.itmo.Config;
import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Comparator;
import java.util.Iterator;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class PaschenkoDao implements Dao<MemorySegment, Entry<MemorySegment>> {

    private final Comparator<MemorySegment> comparator = PaschenkoDao::compare;
    private final NavigableMap<MemorySegment, Entry<MemorySegment>> storage = new ConcurrentSkipListMap<>(comparator);
    private final Arena arena;
    private final MemorySegment readPage;
    private final Path path;

    public PaschenkoDao(Config config) throws IOException {
        this.path = config.basePath().resolve("data.db");

        arena = Arena.ofShared();

        long size;
        try {
            size = Files.size(path);
        } catch (NoSuchFileException e) {
            readPage = null;
            return;
        }

        boolean created = false;
        MemorySegment pageCurrent;
        try (FileChannel fileChannel = FileChannel.open(path, StandardOpenOption.READ)) {
            pageCurrent = fileChannel.map(FileChannel.MapMode.READ_ONLY, 0, size, arena);
            created = true;
        } catch (FileNotFoundException e) {
            pageCurrent = null;
        } finally {
            if (!created) {
                arena.close();
            }
        }

        readPage = pageCurrent;
    }

    private static int compare(MemorySegment memorySegment1, MemorySegment memorySegment2) {
        long mismatch = memorySegment1.mismatch(memorySegment2);
        if (mismatch == -1) {
            return 0;
        }

        if (mismatch == memorySegment1.byteSize()) {
            return -1;
        }

        if (mismatch == memorySegment2.byteSize()) {
            return 1;
        }
        byte b1 = memorySegment1.get(ValueLayout.JAVA_BYTE, mismatch);
        byte b2 = memorySegment2.get(ValueLayout.JAVA_BYTE, mismatch);
        return Byte.compare(b1, b2);
    }

    @Override
    public Iterator<Entry<MemorySegment>> get(MemorySegment from, MemorySegment to) {
        if (from == null && to == null) {
            return storage.values().iterator();
        }
        if (from == null) {
            return storage.headMap(to).values().iterator();
        }
        if (to == null) {
            return storage.tailMap(from).values().iterator();
        }
        return storage.subMap(from, to).values().iterator();
    }

    @Override
    public void upsert(Entry<MemorySegment> entry) {
        storage.put(entry.key(), entry);
    }

    @Override
    public Entry<MemorySegment> get(MemorySegment key) {
        Entry<MemorySegment> entry = storage.get(key);
        if (entry != null) {
            return entry;
        }

        if (readPage == null) {
            return null;
        }

        long offset = 0;

        while (offset < readPage.byteSize()) {
            long keySize = readPage.get(ValueLayout.JAVA_LONG_UNALIGNED, offset);
            offset += Long.BYTES;
            long valueSize = readPage.get(ValueLayout.JAVA_LONG_UNALIGNED, offset);
            offset += Long.BYTES;

            if (keySize != key.byteSize()) {
                offset += keySize + valueSize;
                continue;
            }

            long mismatch = MemorySegment.mismatch(readPage, offset, offset + key.byteSize(), key, 0, key.byteSize());
            if (mismatch == -1) {
                MemorySegment slice = readPage.asSlice(offset + keySize, valueSize);
                return new BaseEntry<>(key, slice);
            }
            offset += keySize + valueSize;
        }

        return null;
    }

    @Override
    public void close() throws IOException {
        if (!arena.scope().isAlive()) {
            return;
        }

        arena.close();

        long size = 0;
        for (Entry<MemorySegment> entry : storage.values()) {
            size += entry.key().byteSize() + entry.value().byteSize();
        }
        size += 2L * storage.size() * Long.BYTES;

        try (Arena writeArena = Arena.ofConfined()) {
            MemorySegment page;
            try (FileChannel fileChannel = FileChannel.open(path,
                    StandardOpenOption.WRITE,
                    StandardOpenOption.READ,
                    StandardOpenOption.TRUNCATE_EXISTING,
                    StandardOpenOption.CREATE)) {
                page = fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, size, writeArena);
            }

            long offset = 0;

            for (Entry<MemorySegment> entry : storage.values()) {
                MemorySegment key = entry.key();
                MemorySegment value = entry.value();

                page.set(ValueLayout.JAVA_LONG_UNALIGNED, offset, key.byteSize());
                offset += Long.BYTES;
                page.set(ValueLayout.JAVA_LONG_UNALIGNED, offset, value.byteSize());
                offset += Long.BYTES;

                MemorySegment.copy(key, 0, page, offset, key.byteSize());
                offset += key.byteSize();

                MemorySegment.copy(value, 0, page, offset, value.byteSize());
                offset += value.byteSize();
            }
        }
    }
}
