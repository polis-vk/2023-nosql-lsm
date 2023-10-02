package ru.vk.itmo.timofeevkirill;

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
import java.nio.file.Path;
import java.util.Comparator;
import java.util.Iterator;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class InMemoryDao implements Dao<MemorySegment, Entry<MemorySegment>> {
    private final Comparator<MemorySegment> comparator = new MemorySegmentComparator();
    private final NavigableMap<MemorySegment, Entry<MemorySegment>> memTableMap =
            new ConcurrentSkipListMap<>(comparator);
    private final Arena readArena = Arena.ofConfined();
    private final MemorySegment readMappedMemorySegment; // SSTable
    private final Path path;

    public InMemoryDao(Config config) {
        this.path = config.basePath().resolve(Constants.FILE_NAME);

        MemorySegment tryReadMappedMemorySegment;
        try {
            tryReadMappedMemorySegment = FileChannel.open(path, Constants.READ_OPTIONS)
                    .map(FileChannel.MapMode.READ_ONLY, 0, Files.size(path), readArena);
        } catch (IOException e) {
            tryReadMappedMemorySegment = null;
        }

        readMappedMemorySegment = tryReadMappedMemorySegment;
    }

    @Override
    public Entry<MemorySegment> get(MemorySegment key) {
        // First trying to return the key from MemTable
        Entry<MemorySegment> entry = memTableMap.get(key);
        if (entry != null) {
            return entry;
        }

        // Trying to return the key from SSTable
        if (readMappedMemorySegment == null) {
            return null;
        }
        long offset = 0;
        while (offset < readMappedMemorySegment.byteSize()) {
            long keySize = readMappedMemorySegment.get(ValueLayout.JAVA_LONG_UNALIGNED, offset);
            offset += Long.BYTES;
            long valueSize = readMappedMemorySegment.get(ValueLayout.JAVA_LONG_UNALIGNED, offset + keySize);

            if (keySize != key.byteSize()) {
                offset += keySize + Long.BYTES + valueSize;
                continue;
            }

            MemorySegment keyMappedMemorySegment = readMappedMemorySegment.asSlice(offset, keySize);
            offset += keySize;
            offset += Long.BYTES;
            if (key.mismatch(keyMappedMemorySegment) == -1) {
                MemorySegment valueMappedMemorySegment = readMappedMemorySegment.asSlice(offset, valueSize);
                return new BaseEntry<>(keyMappedMemorySegment, valueMappedMemorySegment);
            }

            offset += valueSize;
        }

        return null;
    }

    @Override
    public Iterator<Entry<MemorySegment>> get(MemorySegment from, MemorySegment to) {
        if (from == null && to == null) {
            return memTableMap.values().iterator();
        } else if (from == null) {
            return memTableMap.headMap(to).values().iterator();
        } else if (to == null) {
            return memTableMap.tailMap(from).values().iterator();
        } else {
            return memTableMap.tailMap(from).headMap(to).values().iterator();
        }
    }

    @Override
    public void close() throws IOException {
        // Freeing the arena to open a writing channel
        if (!readArena.scope().isAlive()) {
            return;
        }
        readArena.close();
        Arena writeArena = Arena.ofConfined();

        // Calculate the writing size, using all the entries and their sizes
        long mappedMemorySize =
                memTableMap.values().stream().mapToLong(e -> e.key().byteSize() + e.value().byteSize()).sum();
        mappedMemorySize += Long.BYTES * memTableMap.size() * 2L;

        // Memory segment to write
        MemorySegment writeMappedMemorySegment = FileChannel.open(path, Constants.WRITE_OPTIONS)
                .map(FileChannel.MapMode.READ_WRITE, 0, mappedMemorySize, writeArena);

        // Write memTable
        writeMemTableToSSTable(writeMappedMemorySegment);

        writeArena.close();
    }

    private void writeMemTableToSSTable(MemorySegment writeMappedMemorySegment) {
        long offset = 0;
        for (Entry<MemorySegment> entry : memTableMap.values()) {
            MemorySegment key = entry.key();
            writeMappedMemorySegment.set(ValueLayout.JAVA_LONG_UNALIGNED, offset, key.byteSize());
            offset += Long.BYTES;
            writeMappedMemorySegment.asSlice(offset).copyFrom(key);
            offset += key.byteSize();

            MemorySegment value = entry.value();
            writeMappedMemorySegment.set(ValueLayout.JAVA_LONG_UNALIGNED, offset, value.byteSize());
            offset += Long.BYTES;
            writeMappedMemorySegment.asSlice(offset).copyFrom(entry.value());
            offset += value.byteSize();
        }
    }

    @Override
    public void upsert(Entry<MemorySegment> entry) {
        memTableMap.put(entry.key(), entry);
    }

    @Override
    public Iterator<Entry<MemorySegment>> all() {
        return memTableMap.values().iterator();
    }
}
