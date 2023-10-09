package ru.vk.itmo.bazhenovkirill;

import ru.vk.itmo.BaseEntry;
import ru.vk.itmo.Config;
import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class PersistentDaoImpl implements Dao<MemorySegment, Entry<MemorySegment>> {

    private static final String DATA_FILE = "sstable.db";

    private static final Set<StandardOpenOption> WRITE_OPTIONS = Set.of(
            StandardOpenOption.CREATE,
            StandardOpenOption.READ,
            StandardOpenOption.TRUNCATE_EXISTING,
            StandardOpenOption.WRITE
    );

    private final ConcurrentNavigableMap<MemorySegment, Entry<MemorySegment>> memTable
            = new ConcurrentSkipListMap<>(new MemorySegmentComparator());

    private final Path dataPath;

    private final Arena arena;

    public PersistentDaoImpl(Config config) {
        dataPath = config.basePath().resolve(DATA_FILE);
        arena = Arena.ofConfined();
    }

    @Override
    public Iterator<Entry<MemorySegment>> get(MemorySegment from, MemorySegment to) {
        if (from == null) {
            if (to != null) {
                return memTable.headMap(to).values().iterator();
            }
            return memTable.values().iterator();
        } else {
            if (to == null) {
                return memTable.tailMap(from).values().iterator();
            }
            return memTable.subMap(from, true, to, false).values().iterator();
        }
    }

    @Override
    public Entry<MemorySegment> get(MemorySegment key) {
        Entry<MemorySegment> value = memTable.get(key);
        if (value == null) {
            return getDataFromSSTable(key);
        }
        return value;
    }

    @Override
    public void upsert(Entry<MemorySegment> entry) {
        memTable.put(entry.key(), entry);
    }

    @Override
    public void flush() throws IOException {
        try (FileChannel channel = FileChannel.open(dataPath, WRITE_OPTIONS)) {
            try (Arena confinedArena = Arena.ofConfined()) {
                MemorySegment dataMemorySegment = channel.map(MapMode.READ_WRITE, 0,
                        getMemTableSizeInBytes(), confinedArena);
                long offset = 0;
                for (var entry : memTable.values()) {
                    offset = writeEntry(entry, dataMemorySegment, offset);
                }
            }
        }
    }

    @Override
    public void close() throws IOException {
        if (!arena.scope().isAlive()) {
            return;
        }
        arena.close();

        flush();
    }

    private Entry<MemorySegment> getDataFromSSTable(MemorySegment key) {
        if (!Files.exists(dataPath)) {
            return null;
        }

        try (FileChannel channel = FileChannel.open(dataPath, StandardOpenOption.READ)) {
            MemorySegment dataMemorySegment = channel.map(MapMode.READ_ONLY,
                    0, channel.size(), arena).asReadOnly();
            return findElement(dataMemorySegment, key, channel.size());
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
        long updatedOffset = writeDataToMemorySegment(entry.key(), destination, offset);
        return writeDataToMemorySegment(entry.value(), destination, updatedOffset);
    }

    private long writeDataToMemorySegment(MemorySegment entryPart, MemorySegment destination, long offset) {
        long currentOffset = offset;

        destination.set(ValueLayout.JAVA_LONG_UNALIGNED, currentOffset, entryPart.byteSize());
        currentOffset += Long.BYTES;

        MemorySegment.copy(entryPart, 0, destination, currentOffset, entryPart.byteSize());
        currentOffset += entryPart.byteSize();

        return currentOffset;
    }

    public Entry<MemorySegment> findElement(MemorySegment data, MemorySegment key, long fileSize) {
        long offset = 0;
        long valueSize = 0;
        while (offset < fileSize) {
            long keySize = data.get(ValueLayout.JAVA_LONG_UNALIGNED, offset);
            offset += Long.BYTES;

            if (keySize == key.byteSize()) {
                long offsetForCurrentKey = offset;

                offset += keySize;
                valueSize = data.get(ValueLayout.JAVA_LONG_UNALIGNED, offset);
                offset += Long.BYTES;

                MemorySegment possibleKey = data.asSlice(offsetForCurrentKey, keySize);
                if (key.mismatch(possibleKey) == -1) {
                    MemorySegment value = data.asSlice(offset, valueSize);
                    return new BaseEntry<>(possibleKey, value);
                }
            } else {
                offset += keySize;
                valueSize = data.get(ValueLayout.JAVA_LONG_UNALIGNED, offset);
                offset += Long.BYTES;
            }

            offset += valueSize;
        }
        return null;
    }
}
