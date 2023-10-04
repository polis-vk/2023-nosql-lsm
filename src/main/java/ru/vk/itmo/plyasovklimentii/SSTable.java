package ru.vk.itmo.plyasovklimentii;

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

    private static final String SSTABLE_NAME = "sstable.db";
    private static final int LONG_SIZE_BYTES = Long.BYTES;
    private final Path filePath;
    private final MemorySegment segment;
    private final MemoryComparator memoryComparator;

    public SSTable(Config config) throws IOException {
        this.filePath = config.basePath().resolve(SSTABLE_NAME);
        if (!Files.exists(filePath)) {
            segment = null;
            memoryComparator = null;
            return;
        }

        memoryComparator = new MemoryComparator();
        try (FileChannel channel = FileChannel.open(filePath)) {
            segment = channel.map(FileChannel.MapMode.READ_ONLY, 0, channel.size(), Arena.ofConfined());
        }
    }

    public Entry<MemorySegment> get(MemorySegment key) {
        if (segment == null) {
            return null;
        }
        long offset = 0;
        while (offset < segment.byteSize()) {
            long keySize = segment.get(ValueLayout.JAVA_LONG_UNALIGNED, offset);
            offset += LONG_SIZE_BYTES;
            MemorySegment segmentKey = segment.asSlice(offset, keySize);
            offset += keySize;
            long valueSize = segment.get(ValueLayout.JAVA_LONG_UNALIGNED, offset);
            offset += LONG_SIZE_BYTES;

            if (memoryComparator.compare(key, segmentKey) == 0) {
                return new BaseEntry<>(segmentKey, segment.asSlice(offset, valueSize));
            }

            offset += valueSize;
        }
        return null;
    }

    public void save(Map<MemorySegment, Entry<MemorySegment>> storage) throws IOException {
        try (FileChannel channel = FileChannel.open(filePath, StandardOpenOption.TRUNCATE_EXISTING,
                StandardOpenOption.READ, StandardOpenOption.WRITE, StandardOpenOption.CREATE)) {
            long size = 0;

            for (Entry<MemorySegment> entry : storage.values()) {
                MemorySegment key = entry.key();
                MemorySegment value = entry.value();

                size += LONG_SIZE_BYTES + key.byteSize() + LONG_SIZE_BYTES + value.byteSize();
            }

            MemorySegment ssTableSegment = channel.map(FileChannel.MapMode.READ_WRITE, 0, size, Arena.ofConfined());
            long offset = 0;
            for (Entry<MemorySegment> entry : storage.values()) {
                long keySize = entry.key().byteSize();
                ssTableSegment.set(ValueLayout.JAVA_LONG_UNALIGNED, offset, keySize);
                offset += LONG_SIZE_BYTES;
                ssTableSegment.asSlice(offset, keySize).copyFrom(entry.key());
                offset += keySize;

                long valueSize = entry.value().byteSize();
                ssTableSegment.set(ValueLayout.JAVA_LONG_UNALIGNED, offset, valueSize);
                offset += LONG_SIZE_BYTES;
                ssTableSegment.asSlice(offset, valueSize).copyFrom(entry.value());
                offset += valueSize;
            }
        }
    }
}
