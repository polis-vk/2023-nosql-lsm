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
    private final Comparator<MemorySegment> comparator = DaoImpl::compare;
    private final NavigableMap<MemorySegment, Entry<MemorySegment>> storage = new ConcurrentSkipListMap<>(comparator);
    private final NavigableMap<MemorySegment, Entry<MemorySegment>> persistentStorage =
            new ConcurrentSkipListMap<>(comparator);
    private Path pathToSSTable;
    private Arena arena;
    private static final int SIZE_BYTE = Long.BYTES;

    public DaoImpl() {
        // Empty constructor
    }

    public DaoImpl(Config config) throws IOException {
        pathToSSTable = config.basePath().resolve("SSTable");
        arena = Arena.ofShared();
        if (Files.exists(pathToSSTable)) {
            restoreStorage();
        }
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
        return entry == null ? persistentStorage.get(key) : entry;
    }

    @Override
    public synchronized void upsert(Entry<MemorySegment> entry) {
        storage.put(entry.key(), entry);
    }

    @Override
    public void close() throws IOException {
        if (!Files.exists(pathToSSTable)) {
            Files.createFile(pathToSSTable);
        }

        long storageSize = 0;
        for (Entry<MemorySegment> entry : storage.values()) {
            storageSize += SIZE_BYTE + entry.key().byteSize() + SIZE_BYTE + entry.value().byteSize();
        }

        MemorySegment mappedSSTable = mapFileToSegment(storageSize, FileChannel.MapMode.READ_WRITE,
                StandardOpenOption.READ, StandardOpenOption.WRITE);

        long offset = 0;
        for (Entry<MemorySegment> entry : storage.values()) {
            storeSegment(entry.key(), mappedSSTable, offset);
            offset += SIZE_BYTE + entry.key().byteSize();
            storeSegment(entry.value(), mappedSSTable, offset);
            offset += SIZE_BYTE + entry.value().byteSize();
        }
    }

    private MemorySegment getSegment(MemorySegment mappedSSTable, long offset) {
        long size = mappedSSTable.get(ValueLayout.OfLong.JAVA_LONG_UNALIGNED, offset);
        return mappedSSTable.asSlice(offset + SIZE_BYTE, size);
    }

    private void storeSegment(MemorySegment segment, MemorySegment mappedSSTable, long offset) {
        mappedSSTable.set(ValueLayout.OfLong.JAVA_LONG_UNALIGNED, offset, segment.byteSize());
        MemorySegment.copy(segment, 0, mappedSSTable, offset + SIZE_BYTE, segment.byteSize());
    }

    private void restoreStorage() throws IOException {
        MemorySegment mappedSSTable = mapFileToSegment(Files.size(pathToSSTable), FileChannel.MapMode.READ_ONLY,
                StandardOpenOption.READ);

        long offset = 0;
        while (offset < mappedSSTable.byteSize()) {
            MemorySegment key = getSegment(mappedSSTable, offset);
            offset += SIZE_BYTE + key.byteSize();

            MemorySegment value = getSegment(mappedSSTable, offset);
            offset += SIZE_BYTE + value.byteSize();

            persistentStorage.put(key, new BaseEntry<>(key, value));
        }
    }

    private MemorySegment mapFileToSegment(long size, FileChannel.MapMode mapMode, OpenOption... options)
            throws IOException {
        try (FileChannel fileChannel = FileChannel.open(pathToSSTable, options)) {
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
