package ru.vk.itmo.solnyshkoksenia;

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
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Comparator;
import java.util.Iterator;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class DaoImpl implements Dao<MemorySegment, Entry<MemorySegment>> {
    private static final Comparator<MemorySegment> comparator = DaoImpl::compare;
    private final NavigableMap<MemorySegment, Entry<MemorySegment>> storage = new ConcurrentSkipListMap<>(comparator);
    private Config config;
    private Arena arena;

    public DaoImpl() {
        // Empty constructor
    }

    public DaoImpl(Config config) {
        this.config = config;
        arena = Arena.ofShared();
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
    public Entry<MemorySegment> get(MemorySegment key) {
        Entry<MemorySegment> entry = storage.get(key);
        if (entry != null) {
            return entry;
        }

        MemorySegment mappedSSTable;
        try {
            mappedSSTable = mapFileToSegment(Files.size(getPathToSSTable()), FileChannel.MapMode.READ_ONLY, arena,
                    StandardOpenOption.READ);
        } catch (IOException e) {
            return null;
        }

        long offset = 0;
        while (offset < mappedSSTable.byteSize()) {
            MemorySegment persistentKey = getSegment(mappedSSTable, offset);
            offset += Long.BYTES + persistentKey.byteSize();
            MemorySegment persistentValue = getSegment(mappedSSTable, offset);
            offset += Long.BYTES + persistentValue.byteSize();

            if (persistentKey.byteSize() == key.byteSize() && compare(key, persistentKey) == 0) {
                return new BaseEntry<>(persistentKey, persistentValue);
            }
        }
        return null;
    }

    @Override
    public synchronized void upsert(Entry<MemorySegment> entry) {
        storage.put(entry.key(), entry);
    }

    @Override
    public void close() throws IOException {
        arena.close();

        long storageSize = 0;
        for (Entry<MemorySegment> entry : storage.values()) {
            storageSize += Long.BYTES + entry.key().byteSize() + Long.BYTES + entry.value().byteSize();
        }

        try (Arena writeArena = Arena.ofConfined()) {
            MemorySegment mappedSSTable = mapFileToSegment(storageSize, FileChannel.MapMode.READ_WRITE, writeArena,
                    StandardOpenOption.READ, StandardOpenOption.WRITE,
                    StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.CREATE);

            long offset = 0;
            for (Entry<MemorySegment> entry : storage.values()) {
                storeSegment(entry.key(), mappedSSTable, offset);
                offset += Long.BYTES + entry.key().byteSize();
                storeSegment(entry.value(), mappedSSTable, offset);
                offset += Long.BYTES + entry.value().byteSize();
            }
        }
    }

    private Path getPathToSSTable() {
        return config.basePath().resolve("SSTable");
    }

    private MemorySegment getSegment(MemorySegment mappedSSTable, long offset) {
        long size = mappedSSTable.get(ValueLayout.OfLong.JAVA_LONG_UNALIGNED, offset);
        return mappedSSTable.asSlice(offset + Long.BYTES, size);
    }

    private void storeSegment(MemorySegment segment, MemorySegment mappedSSTable, long offset) {
        mappedSSTable.set(ValueLayout.OfLong.JAVA_LONG_UNALIGNED, offset, segment.byteSize());
        MemorySegment.copy(segment, 0, mappedSSTable, offset + Long.BYTES, segment.byteSize());
    }

    private MemorySegment mapFileToSegment(long size, FileChannel.MapMode mapMode, Arena arena, OpenOption... options)
            throws IOException {
        try (FileChannel fileChannel = FileChannel.open(getPathToSSTable(), options)) {
            return fileChannel.map(mapMode, 0, size, arena);
        }
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
}
