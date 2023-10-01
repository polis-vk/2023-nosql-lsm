package ru.vk.itmo.kobyzhevaleksandr;

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
import java.nio.file.NoSuchFileException;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Iterator;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class InMemoryDao implements Dao<MemorySegment, Entry<MemorySegment>> {

    private final NavigableMap<MemorySegment, Entry<MemorySegment>> map =
            new ConcurrentSkipListMap<>(new MemorySegmentComparator());
    private final static String TABLE_FILENAME = "ssTable.dat";

    private final Arena arena = Arena.global();
    private final Config config;
    private final MemorySegment ssTableFile;

    public InMemoryDao() {
        this.config = null;
        this.ssTableFile = null;
    }

    public InMemoryDao(Config config) throws IOException {
        this.config = config;
        var tablePath = getTablePath();
        MemorySegment tableSegment;
        try {
            var size = Files.size(tablePath);
            tableSegment = mapFile(tablePath, size, FileChannel.MapMode.READ_ONLY, StandardOpenOption.READ);
        } catch (NoSuchFileException e) {
            tableSegment = null;
        }
        ssTableFile = tableSegment;
    }

    @Override
    public Iterator<Entry<MemorySegment>> get(MemorySegment from, MemorySegment to) {
        if (from == null && to == null) {
            return map.values().iterator();
        } else if (from == null) {
            return map.headMap(to).values().iterator();
        } else if (to == null) {
            return map.tailMap(from).values().iterator();
        }
        return map.subMap(from, to).values().iterator();
    }

    @Override
    public Entry<MemorySegment> get(MemorySegment key) {
        var entry = map.get(key);
        if (entry != null) {
            return entry;
        }
        if (ssTableFile == null) {
            return null;
        }

        var offset = 0L;
        while (offset < ssTableFile.byteSize()) {
            var keySize = ssTableFile.get(ValueLayout.JAVA_LONG_UNALIGNED, offset);
            offset += Long.BYTES;
            var valueSize = ssTableFile.get(ValueLayout.JAVA_LONG_UNALIGNED, offset + keySize);

            if (keySize != key.byteSize()) {
                offset += keySize + Long.BYTES + valueSize;
                continue;
            }

            if (key.mismatch(ssTableFile.asSlice(offset, keySize)) == -1) {
                return new BaseEntry<>(key, ssTableFile.asSlice(offset + keySize + Long.BYTES, valueSize));
            }
            offset += keySize + Long.BYTES + valueSize;
        }
        return null;
    }

    @Override
    public void upsert(Entry<MemorySegment> entry) {
        if (entry == null) {
            throw new IllegalArgumentException("Entry cannot be null.");
        }
        map.put(entry.key(), entry);
    }

    @Override
    public void close() throws IOException {
        var ssTableSize = 0L;
        long valueSize;
        for (Entry<MemorySegment> entry : map.values()) {
            valueSize = entry.value() == null ? 0 : entry.value().byteSize();
            ssTableSize += Long.BYTES + entry.key().byteSize() + Long.BYTES + valueSize;
        }

        var tablePath = getTablePath();
        Files.deleteIfExists(tablePath);
        Files.createFile(tablePath);
        var newSsTableFile = mapFile(tablePath, ssTableSize, FileChannel.MapMode.READ_WRITE,
            StandardOpenOption.READ, StandardOpenOption.WRITE);

        var offset = 0L;
        for (Entry<MemorySegment> entry : map.values()) {
            offset += writeSegmentInfoToTableFile(newSsTableFile, entry.key(), offset);
            offset += writeSegmentInfoToTableFile(newSsTableFile, entry.value(), offset);
        }
    }

    private Path getTablePath() {
        return config.basePath().resolve(TABLE_FILENAME);
    }

    private MemorySegment mapFile(Path filePath, long bytesSize, FileChannel.MapMode mapMode,
                                  OpenOption... options) throws IOException {
        try (FileChannel fileChannel = FileChannel.open(filePath, options)) {
            return fileChannel.map(mapMode, 0, bytesSize, arena);
        }
    }

    private long writeSegmentInfoToTableFile(MemorySegment tableFile, MemorySegment segment, long offset) {
        if (segment == null) {
            tableFile.set(ValueLayout.JAVA_LONG_UNALIGNED, offset, -1);
            return Long.BYTES;
        }
        tableFile.set(ValueLayout.JAVA_LONG_UNALIGNED, offset, segment.byteSize());
        tableFile.asSlice(offset + Long.BYTES).copyFrom(segment);
        return Long.BYTES + segment.byteSize();
    }
}
