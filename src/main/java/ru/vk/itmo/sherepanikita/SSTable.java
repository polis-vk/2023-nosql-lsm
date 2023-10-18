package ru.vk.itmo.sherepanikita;

import ru.vk.itmo.BaseEntry;
import ru.vk.itmo.Config;
import ru.vk.itmo.Entry;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.NavigableMap;
import java.util.Set;

public class SSTable {

    private static final String FILE_NAME = "ssTable";

    private static final Set<OpenOption> OPTIONS = Set.of(
            StandardOpenOption.CREATE,
            StandardOpenOption.READ,
            StandardOpenOption.WRITE
    );
    private final Path filePath;

    private final Arena arena;

    public SSTable(Config config) {
        this.filePath = config.basePath().resolve(FILE_NAME);
        arena = Arena.ofShared();
    }

    public Entry<MemorySegment> readData(MemorySegment key) throws IOException {
        try (FileChannel fileChannel = FileChannel.open(filePath, StandardOpenOption.READ)) {

            MemorySegment segmentToRead = fileChannel.map(
                    FileChannel.MapMode.READ_ONLY,
                    0L,
                    Files.size(filePath),
                    arena
            ).asReadOnly();

            long offset = 0L;
            while (offset < segmentToRead.byteSize()) {
                long keySize = segmentToRead.get(ValueLayout.JAVA_LONG_UNALIGNED, offset);
                offset += Long.BYTES;
                long valueSize = segmentToRead.get(ValueLayout.JAVA_LONG_UNALIGNED, offset);
                offset += Long.BYTES;

                if (keySize != key.byteSize()) {
                    offset += keySize + valueSize;
                    continue;
                }

                long mismatch = MemorySegment.mismatch(
                        segmentToRead,
                        offset,
                        offset + key.byteSize(),
                        key,
                        0,
                        key.byteSize()
                );
                if (mismatch == -1) {
                    MemorySegment slice = segmentToRead.asSlice(offset + keySize, valueSize);
                    return new BaseEntry<>(key, slice);
                }
                offset += keySize + valueSize;
            }

            return null;
        }
    }

    public void writeGivenInMemoryData(NavigableMap<MemorySegment, Entry<MemorySegment>> inMemoryData)
            throws IOException {
        try (FileChannel fileChannel = FileChannel.open(filePath, OPTIONS);
             Arena writeArena = Arena.ofShared()
        ) {

            if (inMemoryData.isEmpty()) {
                return;
            }

            if (!arena.scope().isAlive()) {
                return;
            }

            arena.close();

            long ssTableSize = 0;

            for (Entry<MemorySegment> entry : inMemoryData.values()) {
                long entryKeySize = entry.key().byteSize();
                long entryValueSize = entry.value().byteSize();
                ssTableSize += Long.BYTES + entryKeySize + Long.BYTES + entryValueSize;
            }

            MemorySegment segmentToWrite = fileChannel.map(
                    FileChannel.MapMode.READ_WRITE,
                    0L,
                    ssTableSize,
                    writeArena
            );

            long offset = 0L;
            for (Entry<MemorySegment> entry : inMemoryData.values()) {
                if (entry == null) {
                    continue;
                }

                long entryKeySize = entry.key().byteSize();
                segmentToWrite.set(ValueLayout.JAVA_LONG_UNALIGNED, offset, entryKeySize);
                offset += Long.BYTES;
                long entryValueSize = entry.value().byteSize();
                segmentToWrite.set(ValueLayout.JAVA_LONG_UNALIGNED, offset, entryValueSize);
                offset += Long.BYTES;

                MemorySegment.copy(entry.key(), 0, segmentToWrite, offset, entryKeySize);
                offset += entryKeySize;
                MemorySegment.copy(entry.value(), 0, segmentToWrite, offset, entryValueSize);
                offset += entryValueSize;
            }

        }
    }
}
