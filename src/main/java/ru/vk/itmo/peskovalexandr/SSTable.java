package ru.vk.itmo.peskovalexandr;

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

    private static final String SSTABLE_FILENAME = "sstable.db";
    private final Path sstableFilePath;

    public SSTable(Config config) {
        sstableFilePath = config.basePath().resolve(SSTABLE_FILENAME);
    }

    public Entry<MemorySegment> get(MemorySegment key) {
        try (
                FileChannel channel = FileChannel.open(
                        sstableFilePath,
                        StandardOpenOption.READ
                )
        ) {
            MemorySegment mappedSegment = channel.map(
                    FileChannel.MapMode.READ_ONLY,
                    0L,
                    Files.size(sstableFilePath),
                    Arena.ofConfined()
            );

            long offset = 0L;
            long size;
            while (offset < mappedSegment.byteSize()) {
                size = mappedSegment.get(ValueLayout.JAVA_LONG_UNALIGNED, offset);
                offset += Long.BYTES;
                MemorySegment keySegment = mappedSegment.asSlice(offset, size);
                offset += size;
                size = mappedSegment.get(ValueLayout.JAVA_LONG_UNALIGNED, offset);
                if (keySegment.mismatch(key) == -1) {
                    MemorySegment valueSegment = mappedSegment.asSlice(offset + Long.BYTES, size);
                    return new BaseEntry<>(keySegment, valueSegment);
                }
                offset += size + Long.BYTES;
            }
        } catch (IOException e) {
            return null;
        }
        return null;
    }

    public void saveEntryMap(Map<MemorySegment, Entry<MemorySegment>> entryMap) throws IOException {
        try (
                FileChannel channel = FileChannel.open(
                        sstableFilePath,
                        StandardOpenOption.TRUNCATE_EXISTING,
                        StandardOpenOption.READ,
                        StandardOpenOption.WRITE,
                        StandardOpenOption.CREATE
                )
        ) {
            MemorySegment mappedSegment = channel.map(
                    FileChannel.MapMode.READ_WRITE,
                    0L,
                    getMapSize(entryMap),
                    Arena.ofConfined()
            );
            long offset = 0L;
            for (Entry<MemorySegment> entry : entryMap.values()) {
                offset = writeMemorySegment(mappedSegment, entry.key(), offset);
                offset = writeMemorySegment(mappedSegment, entry.value(), offset);
            }
            mappedSegment.load();
        }
    }

    private long writeMemorySegment(MemorySegment mappedSegment, MemorySegment writeMemorySegment, long offset) {
        long writeOffset = offset;
        mappedSegment.set(ValueLayout.JAVA_LONG_UNALIGNED, writeOffset, writeMemorySegment.byteSize());
        writeOffset += Long.BYTES;
        MemorySegment.copy(writeMemorySegment, 0, mappedSegment, writeOffset, writeMemorySegment.byteSize());
        writeOffset += writeMemorySegment.byteSize();
        return writeOffset;
    }

    private long getMapSize(Map<MemorySegment, Entry<MemorySegment>> entryMap) {
        long mapSize = entryMap.size() * Long.BYTES * 2L;
        for (Entry<MemorySegment> entry : entryMap.values()) {
            mapSize += entry.key().byteSize() + entry.value().byteSize();
        }
        return mapSize;
    }
}
