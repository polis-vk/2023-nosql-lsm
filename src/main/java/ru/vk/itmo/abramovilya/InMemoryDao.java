package ru.vk.itmo.abramovilya;

import ru.vk.itmo.BaseEntry;
import ru.vk.itmo.Config;
import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Iterator;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class InMemoryDao implements Dao<MemorySegment, Entry<MemorySegment>> {
    private final ConcurrentNavigableMap<MemorySegment, Entry<MemorySegment>> map =
            new ConcurrentSkipListMap<>(InMemoryDao::compareMemorySegments);
    private final Path storagePath;
    private final Arena arena = Arena.ofConfined();
    private final MemorySegment mappedStorageFile;
    private final FileChannel fileChannel;
    private final long storageSize;

    public InMemoryDao(Config config) {
        storagePath = config.basePath().resolve("storage");

        if (Files.exists(storagePath)) {
            try {
                fileChannel = FileChannel.open(storagePath, StandardOpenOption.READ);
                mappedStorageFile = fileChannel.map(FileChannel.MapMode.READ_ONLY, 0, Files.size(storagePath), arena);
                storageSize = Files.size(storagePath);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        } else {
            fileChannel = null;
            mappedStorageFile = null;
            storageSize = 0;
        }
    }

    @Override
    public Iterator<Entry<MemorySegment>> get(MemorySegment from, MemorySegment to) {
        ConcurrentNavigableMap<MemorySegment, Entry<MemorySegment>> subMap;
        if (from == null && to == null) {
            subMap = map;
        } else if (from == null) {
            subMap = map.headMap(to);
        } else if (to == null) {
            subMap = map.tailMap(from);
        } else {
            subMap = map.subMap(from, to);
        }
        return subMap.values().iterator();
    }

    @Override
    public void upsert(Entry<MemorySegment> entry) {
        map.put(entry.key(), entry);
    }

    @Override
    public Entry<MemorySegment> get(MemorySegment key) {
        var value = map.get(key);
        if (value != null) {
            return value;
        }
        return seekForValueInFile(key);
    }

    private Entry<MemorySegment> seekForValueInFile(MemorySegment key) {
        if (mappedStorageFile == null) {
            return null;
        }
        long readOffset = 0;
        while (readOffset < storageSize) {
            long keySize = mappedStorageFile.get(ValueLayout.JAVA_LONG_UNALIGNED, readOffset);
            readOffset += Long.BYTES;
            MemorySegment keySegment = mappedStorageFile.asSlice(readOffset, keySize);
            readOffset += keySize;
            if (compareMemorySegments(key, keySegment) == 0) {
                long valueSize = mappedStorageFile.get(ValueLayout.JAVA_LONG_UNALIGNED, readOffset);
                readOffset += Long.BYTES;
                return new BaseEntry<>(key, mappedStorageFile.asSlice(readOffset, valueSize));
            }
        }
        return null;
    }

    @Override
    public void flush() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void close() throws IOException {
        if (arena.scope().isAlive()) {
            arena.close();
        }
        if (fileChannel != null && fileChannel.isOpen()) {
            fileChannel.close();
        }
        writeMapIntoFile();
    }

    private void writeMapIntoFile() throws IOException {
        long writeOffset = 0;
        try (var channel = FileChannel.open(storagePath,
                StandardOpenOption.READ,
                StandardOpenOption.WRITE,
                StandardOpenOption.TRUNCATE_EXISTING,
                StandardOpenOption.CREATE);
             var writeArena = Arena.ofConfined()) {
            MemorySegment mapped = channel.map(FileChannel.MapMode.READ_WRITE, 0, calcMapByteSizeInFile(), writeArena);
            for (var entry : map.values()) {
                writeOffset += 2 * Long.BYTES;
                writeOffset += entry.key().byteSize();
                writeOffset += entry.value().byteSize();
                writeMemorySegment(entry.key(), mapped, writeOffset);
                writeMemorySegment(entry.value(), mapped, writeOffset);
            }
            mapped.load();
        }
    }

    private long calcMapByteSizeInFile() {
        long size = 0;
        for (var entry : map.values()) {
            size += 2 * Long.BYTES;
            size += entry.key().byteSize();
            size += entry.value().byteSize();
        }
        return size;
    }

    // Every memorySegment in file has the following structure:
    // 8 bytes - size, <size> bytes - value
    private void writeMemorySegment(MemorySegment memorySegment, MemorySegment mapped, long writeOffset) {
        long msSize = memorySegment.byteSize();
        mapped.set(ValueLayout.JAVA_LONG_UNALIGNED, writeOffset, msSize);
        MemorySegment.copy(memorySegment, 0, mapped, writeOffset, msSize);
    }

    private static int compareMemorySegments(MemorySegment segment1, MemorySegment segment2) {
        long offset = segment1.mismatch(segment2);
        if (offset == -1) {
            return 0;
        } else if (offset == segment1.byteSize()) {
            return -1;
        } else if (offset == segment2.byteSize()) {
            return 1;
        }
        return Byte.compare(
                segment1.get(ValueLayout.JAVA_BYTE, offset),
                segment2.get(ValueLayout.JAVA_BYTE, offset)
        );
    }
}
