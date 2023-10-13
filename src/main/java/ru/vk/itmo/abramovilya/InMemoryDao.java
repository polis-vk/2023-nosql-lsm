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
import java.util.stream.Collectors;

public class InMemoryDao implements Dao<MemorySegment, Entry<MemorySegment>> {
    private final ConcurrentNavigableMap<MemorySegment, Entry<MemorySegment>> map =
            new ConcurrentSkipListMap<>(InMemoryDao::compareMemorySegments);
    private final Path storagePath;
    private final Arena arena = Arena.ofShared();
    private long readOffset;
    private long writeOffset;
    private final String sstableBaseName = "storage";
    private final String indexBaseName = "index";
    private final Path metaFilePath;

    public InMemoryDao(Config config) {
        storagePath = config.basePath();

        try {
            metaFilePath = storagePath.resolve("meta");
            if (!Files.exists(metaFilePath)) {
                Files.createFile(metaFilePath);
            }
            Files.writeString(metaFilePath, "0");
        } catch (IOException e) {
            throw new UncheckedIOException(e);
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

        try {
            int totalSStables = getTotalSStables();
            for (int sstableNum = totalSStables; sstableNum >= 0; sstableNum--) {
                Path currSStablePath = storagePath.resolve(sstableBaseName + sstableNum);
                var foundEntry = seekForValueInFile(key, currSStablePath);
                if (foundEntry != null) {
                    return foundEntry;
                }
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return null;
    }

    private Entry<MemorySegment> seekForValueInFile(MemorySegment key, Path filePath) {
        if (!Files.exists(filePath)) {
            return null;
        }
        readOffset = 0;
        try (FileChannel fc = FileChannel.open(filePath, StandardOpenOption.READ)) {
            MemorySegment mapped = fc.map(FileChannel.MapMode.READ_ONLY, 0, Files.size(filePath), arena);
            while (readOffset < Files.size(filePath)) {
                MemorySegment keySegment = getMemorySegment(mapped);
                if (compareMemorySegments(key, keySegment) == 0) {
                    return new BaseEntry<>(key, getMemorySegment(mapped));
                }
            }
            return null;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private MemorySegment getMemorySegment(MemorySegment mappedMemory) throws IOException {
        long size = mappedMemory.get(ValueLayout.JAVA_LONG_UNALIGNED, readOffset);
        readOffset += Long.BYTES;
        MemorySegment memorySegment = mappedMemory.asSlice(readOffset, size);
        readOffset += size;
        return memorySegment;
    }

    @Override
    public void flush() throws IOException {
        writeMapIntoFile();
        incTotalSStablesAmount();
    }

    private void incTotalSStablesAmount() throws IOException {
        int totalSStables = getTotalSStables();
        Files.writeString(metaFilePath, String.valueOf(totalSStables + 1));
    }

    @Override
    public void close() throws IOException {
        if (arena.scope().isAlive()) {
            arena.close();
        }
        flush();
    }

    private void writeMapIntoFile() throws IOException {
        int currSStableNum = getTotalSStables() + 1;
        Path sstablePath = storagePath.resolve(sstableBaseName + currSStableNum);

        writeOffset = 0;
        try (var storageChannel = FileChannel.open(sstablePath,
                StandardOpenOption.READ,
                StandardOpenOption.WRITE,
                StandardOpenOption.CREATE);
             var indexChannel = FileChannel.open(sstablePath,
                     StandardOpenOption.READ,
                     StandardOpenOption.WRITE,
                     StandardOpenOption.CREATE);
             var storageArena = Arena.ofConfined();
             var indexArena = Arena.ofConfined()) {
            MemorySegment mappedStorage = storageChannel.map(FileChannel.MapMode.READ_WRITE, 0, calcMapByteSizeInFile(), storageArena);
            MemorySegment mappedIndex = indexChannel.map(FileChannel.MapMode.READ_WRITE, 0, calcIndexByteSizeInFile(), indexArena);
            for (var entry : map.values()) {
                writeMemorySegment(entry.key(), mappedStorage);
                writeMemorySegment(entry.value(), mappedStorage);
            }
            mappedStorage.load();
        }
    }


    private int getTotalSStables() throws IOException {
        return Integer.parseInt(Files.readString(metaFilePath));
//        return 0;
    }
    private long calcIndexByteSizeInFile() {
        return map.keySet().stream().mapToLong(k -> k.byteSize() + Long.BYTES).sum();
    }

    private long calcMapByteSizeInFile() {
        long size = 0;
        for (var entry : map.values()) {
            size += 2 * Long.BYTES;
            size += entry.key().byteSize();
            if (entry.value() != null) {
                size += entry.value().byteSize();
            }
        }
        return size;
    }

    // Every memorySegment in file has the following structure:
    // 8 bytes - size, <size> bytes - value
    // If memorySegment has the size of 0 bytes, then it means its value is DELETED
    private void writeMemorySegment(MemorySegment memorySegment, MemorySegment mapped) {
        if (memorySegment != null) {
            long msSize = memorySegment.byteSize();
            mapped.set(ValueLayout.JAVA_LONG_UNALIGNED, writeOffset, msSize);
            writeOffset += Long.BYTES;
            MemorySegment.copy(memorySegment, 0, mapped, writeOffset, msSize);
            writeOffset += msSize;
        }
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
