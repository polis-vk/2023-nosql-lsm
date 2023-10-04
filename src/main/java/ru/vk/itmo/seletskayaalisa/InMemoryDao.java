package ru.vk.itmo.seletskayaalisa;

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
import java.util.Iterator;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class InMemoryDao implements Dao<MemorySegment, Entry<MemorySegment>> {
    private static final String FILE_NAME = "entries";
    private final NavigableMap<MemorySegment, Entry<MemorySegment>> segmentsMap;
    private Path basePath;

    public InMemoryDao() {
        segmentsMap = new ConcurrentSkipListMap<>(new MemorySegmentComparator());
    }

    public InMemoryDao(Config config) {
        this();
        this.basePath = config.basePath();
    }

    @Override
    public void flush() throws IOException {
        int size = segmentsMap.size();

        if (size == 0) {
            return;
        }

        long fileSize = 0;

        for (Entry<MemorySegment> entry : segmentsMap.values()) {
            fileSize += Long.BYTES + entry.key().byteSize() + Long.BYTES + entry.value().byteSize();
        }

        Path filePath = basePath.resolve(FILE_NAME);

        if (Files.notExists(filePath)) {
            Files.createFile(filePath);
        }

        MemorySegment mappedSegment = mapFile(filePath, fileSize,
                FileChannel.MapMode.READ_WRITE, StandardOpenOption.READ, StandardOpenOption.WRITE);

        long offset = 0;

        for (Entry<MemorySegment> entry : segmentsMap.values()) {
            long keySize = entry.key().byteSize();

            mappedSegment.set(ValueLayout.JAVA_LONG_UNALIGNED, offset, keySize);
            offset += Long.BYTES;
            mappedSegment.asSlice(offset, keySize).copyFrom(entry.key());
            offset += keySize;

            long valueSize = entry.value().byteSize();
            mappedSegment.set(ValueLayout.JAVA_LONG_UNALIGNED, offset, valueSize);
            offset += Long.BYTES;
            mappedSegment.asSlice(offset, valueSize).copyFrom(entry.value());
            offset += valueSize;
        }
    }

    @Override
    public Entry<MemorySegment> get(MemorySegment key) {
        if (segmentsMap.containsKey(key)) {
            return segmentsMap.get(key);
        } else {
            try {
                return getFromFile(key);
            } catch (IOException e) {
                return null;
            }
        }
    }

    @Override
    public Iterator<Entry<MemorySegment>> get(MemorySegment from, MemorySegment to) {
        if (from == null && to == null) {
            return segmentsMap.values().iterator();
        }
        if (from == null) {
            return segmentsMap.headMap(to).values().iterator();
        }
        if (to == null) {
            return segmentsMap.tailMap(from).values().iterator();
        }
        return segmentsMap.subMap(from, to).values().iterator();
    }

    @Override
    public void upsert(Entry<MemorySegment> entry) {
        if (entry == null) {
            throw new IllegalArgumentException("The provided entry is null");
        }
        segmentsMap.put(entry.key(), entry);
    }

    public Entry<MemorySegment> getFromFile(MemorySegment key) throws IOException {
        Path filePath = basePath.resolve(FILE_NAME);

        if (Files.notExists(filePath)) {
            return null;
        }

        long fileSize = Files.size(filePath);

        MemorySegment mappedSegment = mapFile(filePath, fileSize,
                FileChannel.MapMode.READ_ONLY, StandardOpenOption.READ);

        MemorySegmentComparator comparator = new MemorySegmentComparator();

        long offset = 0;

        while (offset < fileSize) {
            long keySize = mappedSegment.get(ValueLayout.JAVA_LONG_UNALIGNED, offset);
            offset += Long.BYTES;
            MemorySegment entryKey = mappedSegment.asSlice(offset, keySize);
            offset += keySize;

            long valueSize = mappedSegment.get(ValueLayout.JAVA_LONG_UNALIGNED, offset);
            offset += Long.BYTES;
            MemorySegment entryValue = mappedSegment.asSlice(offset, valueSize);
            offset += valueSize;

            if (comparator.compare(key, entryKey) == 0) {
                return new BaseEntry<>(entryKey, entryValue);
            }
        }

        return null;
    }

    private MemorySegment mapFile(Path filePath, long byteSize, FileChannel.MapMode mode,
                                  OpenOption... channelOptions) throws IOException {
        try (FileChannel fileChannel = FileChannel.open(filePath, channelOptions)) {
            return fileChannel.map(mode, 0, byteSize, Arena.ofConfined());
        }
    }
}
