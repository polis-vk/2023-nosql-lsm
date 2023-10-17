package ru.vk.itmo.volkovnikita;

import ru.vk.itmo.BaseEntry;
import ru.vk.itmo.Config;
import ru.vk.itmo.Entry;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.NavigableMap;

public class SSTable {

    private final Path basePath;
    private static final String FILE_PATH = "SSTable";
    private final MemorySegment readSegment;
    private final Arena arena;

    public SSTable(Config config) throws IOException {
        this.basePath = config.basePath().resolve(FILE_PATH);

        arena = Arena.ofShared();

        long size;
        try {
            size = Files.size(basePath);
        } catch (NoSuchFileException e) {
            readSegment = null;
            return;
        }

        boolean created = false;
        MemorySegment pageCurrent;
        try (FileChannel fileChannel = FileChannel.open(basePath, StandardOpenOption.READ)) {
            pageCurrent = fileChannel.map(FileChannel.MapMode.READ_ONLY, 0, size, arena);
            created = true;
        } catch (FileNotFoundException e) {
            pageCurrent = null;
        } finally {
            if (!created) {
                arena.close();
            }
        }

        readSegment = pageCurrent;
    }

    public void saveMemoryData(NavigableMap<MemorySegment, Entry<MemorySegment>> memorySegmentEntries)
            throws IOException {
        if (!arena.scope().isAlive()) {
            return;
        }

        arena.close();

        long mappedMemorySize =
                memorySegmentEntries.values().stream().mapToLong(e -> e.key().byteSize() + e.value().byteSize()).sum()
                        + Long.BYTES * memorySegmentEntries.size() * 2L;

        try (FileChannel fileChannel = FileChannel.open(basePath,
                StandardOpenOption.READ,
                StandardOpenOption.WRITE,
                StandardOpenOption.TRUNCATE_EXISTING,
                StandardOpenOption.CREATE);
             Arena writeArena = Arena.ofConfined()) {
            MemorySegment seg = fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, mappedMemorySize, writeArena);

            long offset = 0L;

            for (Entry<MemorySegment> entry : memorySegmentEntries.values()) {
                MemorySegment key = entry.key();
                seg.set(ValueLayout.JAVA_LONG_UNALIGNED, offset, key.byteSize());
                offset += Long.BYTES;
                MemorySegment.copy(key, 0, seg, offset, key.byteSize());
                offset += entry.key().byteSize();

                MemorySegment value = entry.value();
                seg.set(ValueLayout.JAVA_LONG_UNALIGNED, offset, value.byteSize());
                offset += Long.BYTES;
                MemorySegment.copy(value, 0, seg, offset, value.byteSize());
                offset += entry.value().byteSize();
            }
        }
    }

    public Entry<MemorySegment> readMemoryData(MemorySegment key) {
        if (basePath == null || !Files.exists(basePath)) {
            return null;
        }

        long offset = 0L;
        while (offset < readSegment.byteSize()) {
            long keySize = readSegment.get(ValueLayout.JAVA_LONG_UNALIGNED, offset);
            offset += Long.BYTES;

            long valueSize = readSegment.get(ValueLayout.JAVA_LONG_UNALIGNED, offset + keySize);

            if (keySize != key.byteSize()) {
                offset += keySize + valueSize + Long.BYTES;
                continue;
            }

            MemorySegment keySegment = readSegment.asSlice(offset, keySize);
            offset += keySize + Long.BYTES;

            if (key.mismatch(keySegment) == -1) {
                MemorySegment valueSegment = readSegment.asSlice(offset, valueSize);
                return new BaseEntry<>(keySegment, valueSegment);
            }

            offset += valueSize;
        }
        return null;
    }
}
