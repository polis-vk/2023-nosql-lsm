package ru.vk.itmo.bazhenovkirill;

import ru.vk.itmo.Config;
import ru.vk.itmo.Entry;
import ru.vk.itmo.bazhenovkirill.strategy.ElementSearchStrategy;
import ru.vk.itmo.bazhenovkirill.strategy.LinearSearchStrategy;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Set;

public class PersistentDaoImpl extends InMemoryDaoImpl {

    private static final String DATA_FILE = "sstable.db";

    private static final ElementSearchStrategy searchStrategy = new LinearSearchStrategy();

    private static final Set<StandardOpenOption> WRITE_OPTIONS = Set.of(
            StandardOpenOption.CREATE,
            StandardOpenOption.READ,
            StandardOpenOption.TRUNCATE_EXISTING,
            StandardOpenOption.WRITE
    );

    private final Path dataPath;

    public PersistentDaoImpl(Config config) {
        dataPath = config.basePath().resolve(DATA_FILE);
    }

    @Override
    public Entry<MemorySegment> get(MemorySegment key) {
        Entry<MemorySegment> value = memTable.get(key);
        if (value == null && Files.exists(dataPath)) {
            return getDataFromSSTable(key);
        }
        return value;
    }

    @Override
    public void flush() throws IOException {
        try (FileChannel channel = FileChannel.open(dataPath, WRITE_OPTIONS)) {
            try (Arena arena = Arena.ofConfined()) {
                MemorySegment dataMemorySegment = channel.map(MapMode.READ_WRITE, 0, getMemTableSizeInBytes(), arena);
                long offset = 0;
                for (var entry : memTable.values()) {
                    offset = writeEntry(entry, dataMemorySegment, offset);
                }
            }
        }
    }

    private Entry<MemorySegment> getDataFromSSTable(MemorySegment key) {
        try (FileChannel channel = FileChannel.open(dataPath, StandardOpenOption.READ)) {
            MemorySegment dataMemorySegment = channel.map(MapMode.READ_ONLY, 0, channel.size(), Arena.ofAuto());
            return searchStrategy.search(dataMemorySegment, key, channel.size());
        } catch (IOException e) {
            return null;
        }
    }

    private long getMemTableSizeInBytes() {
        long size = memTable.size() * Long.BYTES * 2L;
        for (var entry : memTable.values()) {
            size += entry.key().byteSize() + entry.value().byteSize();
        }
        return size;
    }

    private long writeEntry(Entry<MemorySegment> entry, MemorySegment destination, long offset) {
        long updatedOffset = writeEntryKey(entry.key(), destination, offset);
        return writeEntryKey(entry.value(), destination, updatedOffset);
    }

    private long writeEntryKey(MemorySegment entryPart, MemorySegment destination, long offset) {
        long currentOffset = offset;

        destination.set(ValueLayout.JAVA_LONG_UNALIGNED, currentOffset, entryPart.byteSize());
        currentOffset += Long.BYTES;

        destination.asSlice(currentOffset).copyFrom(entryPart);
        currentOffset += entryPart.byteSize();

        return currentOffset;
    }
}
