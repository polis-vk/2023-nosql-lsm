package ru.vk.itmo.alginavictoria;

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

import static java.nio.channels.FileChannel.MapMode.READ_WRITE;

public class PersistenceDaoImpl extends AbstractDao {

    private final Arena arena;
    private static final String DATA_FILE_NAME = "sstable";
    private final Path tablePath;

    public PersistenceDaoImpl(Config config) {
        tablePath = config.basePath().resolve(DATA_FILE_NAME);
        arena = Arena.ofConfined();
    }

    @Override
    public void flush() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Entry<MemorySegment> get(MemorySegment key) {

        if (dataMap.containsKey(key)) {
            return dataMap.get(key);
        }
        if (tablePath == null || !Files.exists(tablePath)) {
            return null;
        }

        try (FileChannel channel = FileChannel.open(tablePath, StandardOpenOption.READ)) {
            MemorySegment memorySegment = channel.map(
                    FileChannel.MapMode.READ_ONLY, 0, Files.size(tablePath),
                    Arena.ofShared());

            long offset = 0;
            long keySize;
            long valueSize;
            while (offset < memorySegment.byteSize()) {
                keySize = memorySegment.get(ValueLayout.JAVA_LONG_UNALIGNED, offset);
                MemorySegment keySegment = memorySegment.asSlice(offset + Long.BYTES, keySize);
                offset += keySize + Long.BYTES;
                valueSize = memorySegment.get(ValueLayout.JAVA_LONG_UNALIGNED, offset);
                if (keySegment.mismatch(key) == -1) {
                    return new BaseEntry<>(keySegment, memorySegment.asSlice(offset + Long.BYTES, valueSize));
                }
                offset += valueSize + Long.BYTES;
            }
            return null;
        } catch (IOException e) {
            return null;
        }
    }

    @Override
    public void close() throws IOException {
        if (!arena.scope().isAlive()) {
            return;
        }
        try (FileChannel fileChannel = FileChannel.open(tablePath, StandardOpenOption.TRUNCATE_EXISTING,
                StandardOpenOption.READ, StandardOpenOption.WRITE, StandardOpenOption.CREATE)) {
            long size = Long.BYTES * 2L * dataMap.size();
            for (Entry<MemorySegment> entry : dataMap.values()) {
                size += entry.key().byteSize() + entry.value().byteSize();
            }
            MemorySegment memorySegment = fileChannel.map(READ_WRITE, 0, size, Arena.ofConfined());
            long offset = 0;
            for (Entry<MemorySegment> entry : dataMap.values()) {
                memorySegment.set(ValueLayout.JAVA_LONG_UNALIGNED, offset, entry.key().byteSize());
                offset += Long.BYTES;

                memorySegment.asSlice(offset, entry.key().byteSize()).copyFrom(entry.key());
                offset += entry.key().byteSize();

                memorySegment.set(ValueLayout.JAVA_LONG_UNALIGNED, offset, entry.value().byteSize());
                offset += Long.BYTES;

                memorySegment.asSlice(offset, entry.value().byteSize()).copyFrom(entry.value());
                offset += entry.value().byteSize();
            }
        }
    }
    
}
