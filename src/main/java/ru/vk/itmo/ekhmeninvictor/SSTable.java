package ru.vk.itmo.ekhmeninvictor;

import ru.vk.itmo.BaseEntry;
import ru.vk.itmo.Config;
import ru.vk.itmo.Entry;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Map;

public class SSTable {

    private final MemorySegmentComparator memorySegmentComparator = new MemorySegmentComparator();
    private final Path path;
    private final MemorySegment memorySegment;
    private static final String SS_TABLE_NAME = "storage";

    public SSTable(Config config) throws IOException {
        this.path = config.basePath().resolve(SS_TABLE_NAME);
        if (!Files.exists(path)) {
            memorySegment = null;
            return;
        }
        try (var fileChannel = FileChannel.open(path)) {
            Arena arena = Arena.ofConfined();
            memorySegment = fileChannel.map(FileChannel.MapMode.READ_ONLY, 0, fileChannel.size(), arena);
        }
    }

    public Entry<MemorySegment> get(MemorySegment key) {
        if (memorySegment == null) {
            return null;
        }
        long offset = 0;
        while (offset < memorySegment.byteSize()) {
            long keySize = memorySegment.get(ValueLayout.JAVA_LONG_UNALIGNED, offset);
            offset += Long.BYTES;
            MemorySegment segment = memorySegment.asSlice(offset, keySize);
            offset += keySize;

            long valueSize = memorySegment.get(ValueLayout.JAVA_LONG_UNALIGNED, offset);
            offset += Long.BYTES;
            if (memorySegmentComparator.compare(key, segment) == 0) {
                return new BaseEntry<>(segment, memorySegment.asSlice(offset, valueSize));
            }
            offset += valueSize;
        }
        return null;
    }

    public void save(Map<MemorySegment, Entry<MemorySegment>> cache) throws IOException {
        if (cache.isEmpty()) {
            return;
        }
        try (var fileChannel = FileChannel.open(
                path,
                StandardOpenOption.READ,
                StandardOpenOption.WRITE,
                StandardOpenOption.CREATE,
                StandardOpenOption.TRUNCATE_EXISTING
        )) {
            long size = 0;
            for (var entry : cache.values()) {
                MemorySegment key = entry.key();
                MemorySegment value = entry.value();
                size += Long.BYTES + key.byteSize() + Long.BYTES + value.byteSize();
            }

            MemorySegment tableSegment = fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, size, Arena.ofConfined());
            long offset = 0;
            for (var entry : cache.values()) {
                MemorySegment key = entry.key();
                long keySize = key.byteSize();
                tableSegment.set(ValueLayout.JAVA_LONG_UNALIGNED, offset, keySize);
                offset += Long.BYTES;
                tableSegment.asSlice(offset, keySize).copyFrom(key);
                offset += keySize;

                MemorySegment value = entry.value();
                long valueSize = value.byteSize();
                tableSegment.set(ValueLayout.JAVA_LONG_UNALIGNED, offset, valueSize);
                offset += Long.BYTES;
                tableSegment.asSlice(offset, valueSize).copyFrom(value);
                offset += valueSize;
            }
        }
    }
}
