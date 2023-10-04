package ru.vk.itmo.supriadkinadaria;

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

import static java.nio.channels.FileChannel.MapMode.READ_ONLY;
import static java.nio.channels.FileChannel.MapMode.READ_WRITE;

public class SStableManipulations {
    private static final String SSTABLE = "sstable.db";
    private final Path filePath;

    public SStableManipulations(Config config) {
        this.filePath = config.basePath().resolve(SSTABLE);
    }

    public Path getFilePath() {
        return filePath;
    }

    public void readStorage(InMemoryDao dao) throws IOException {
        try (FileChannel channel = FileChannel.open(filePath, StandardOpenOption.READ)) {
            long fileSize = Files.size(filePath);
            MemorySegment storageSegment = channel.map(READ_ONLY, 0, fileSize, Arena.ofConfined());
            long offset = 0;
            while (offset < fileSize) {
                long keySize = storageSegment.get(ValueLayout.JAVA_LONG_UNALIGNED, offset);
                offset += Long.BYTES;
                MemorySegment key = storageSegment.asSlice(offset, keySize);
                offset += keySize;
                long valueSize = storageSegment.get(ValueLayout.JAVA_LONG_UNALIGNED, offset);
                offset += Long.BYTES;
                dao.upsert(new BaseEntry<>(key, storageSegment.asSlice(offset, valueSize)));
                offset += valueSize;
            }
        }
    }

    public void writeToFile(Map<MemorySegment, Entry<MemorySegment>> storage) throws IOException {
        try (FileChannel channel = FileChannel.open(filePath, StandardOpenOption.TRUNCATE_EXISTING,
                StandardOpenOption.READ, StandardOpenOption.WRITE, StandardOpenOption.CREATE)) {
            long size = Long.BYTES * 2L * storage.size();
            for (Entry<MemorySegment> entry : storage.values()) {
                size += entry.key().byteSize() + entry.value().byteSize();
            }
            MemorySegment storageSegment = channel.map(READ_WRITE, 0, size, Arena.ofConfined());
            long offset = 0;
            for (Entry<MemorySegment> entry : storage.values()) {
                storageSegment.set(ValueLayout.JAVA_LONG_UNALIGNED, offset, entry.key().byteSize());
                offset += Long.BYTES;
                storageSegment.asSlice(offset, entry.key().byteSize()).copyFrom(entry.key());
                offset += entry.key().byteSize();
                storageSegment.set(ValueLayout.JAVA_LONG_UNALIGNED, offset, entry.value().byteSize());
                offset += Long.BYTES;
                storageSegment.asSlice(offset, entry.value().byteSize()).copyFrom(entry.value());
                offset += entry.value().byteSize();
            }
        }
    }
}
