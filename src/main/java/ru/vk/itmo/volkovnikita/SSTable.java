package ru.vk.itmo.volkovnikita;

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
import java.util.NavigableMap;

public class SSTable {

    private final Path basePath;
    private static final String FILE_PATH = "SSTable";
    private final FileChannel fc;

    public SSTable(Config config) throws IOException {
        this.basePath = config.basePath().resolve(FILE_PATH);
        fc = FileChannel.open(basePath, StandardOpenOption.READ);
    }

    public void saveMemoryData(NavigableMap<MemorySegment, Entry<MemorySegment>> memorySegmentEntries)
            throws IOException {
        long offset = 0L;
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

            for (Entry<MemorySegment> entry : memorySegmentEntries.values()) {
                seg.set(ValueLayout.JAVA_LONG_UNALIGNED, offset, entry.key().byteSize());
                offset += Long.BYTES;
                seg.asSlice(offset).copyFrom(entry.key());
                offset += entry.key().byteSize();

                seg.set(ValueLayout.JAVA_LONG_UNALIGNED, offset, entry.value().byteSize());
                offset += Long.BYTES;
                seg.asSlice(offset).copyFrom(entry.value());
                offset += entry.value().byteSize();
            }
        }
    }

    public Entry<MemorySegment> readMemoryData(MemorySegment key) {
        if (basePath == null || !Files.exists(basePath)) {
            return null;
        }

        try {
            long offset = 0L;
            MemorySegment readSegment = fc.map(FileChannel.MapMode.READ_ONLY, 0, Files.size(basePath), Arena.ofAuto());
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
        } catch (IOException e) {
            return null;
        }
    }
}
