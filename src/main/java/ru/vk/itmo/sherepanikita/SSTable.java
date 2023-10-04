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

    private final Path filePath;
    private static final String FILE_NAME = "ssTable";

    private final Set<OpenOption> options = Set.of(
            StandardOpenOption.CREATE,
            StandardOpenOption.READ,
            StandardOpenOption.WRITE
    );

    public SSTable(Config config) {
        this.filePath = config.basePath().resolve(FILE_NAME);
    }

    public Entry<MemorySegment> readData(MemorySegment key) throws IOException {
        try (FileChannel fileChannel = FileChannel.open(filePath, StandardOpenOption.READ)) {

            MemorySegment segmentToRead = fileChannel.map(
                    FileChannel.MapMode.READ_ONLY,
                    0L,
                    Files.size(filePath),
                    Arena.global()
            );

            long offset = 0L;
            while (offset < segmentToRead.byteSize()) {
                long keySize = segmentToRead.get(ValueLayout.JAVA_LONG_UNALIGNED, offset);
                offset += Long.BYTES + keySize;
                long valueSize = segmentToRead.get(ValueLayout.JAVA_LONG_UNALIGNED, offset);

                if (keySize != key.byteSize()) {
                    offset += Long.BYTES + valueSize;
                    continue;
                }

                MemorySegment keySegment = segmentToRead.asSlice(offset, keySize);

                if (key.mismatch(keySegment) == -1) {
                    MemorySegment valueSegment = segmentToRead.asSlice(
                            offset + Long.BYTES,
                            valueSize
                    );
                    return new BaseEntry<>(keySegment, valueSegment);
                }

                offset += Long.BYTES + valueSize;
            }

            return null;
        }
    }

    public void writeGivenInMemoryData(NavigableMap<MemorySegment, Entry<MemorySegment>> inMemoryData) throws IOException {
        try (FileChannel fileChannel = FileChannel.open(filePath, options)) {

            for (Entry<MemorySegment> entry : inMemoryData.values()) {
                long entryValueSize;
                long entryKeySize;
                long ssTableSize;

                if (entry != null) {
                    entryValueSize = entry.value().byteSize();
                    entryKeySize = entry.key().byteSize();
                } else {
                    entryValueSize = 0L;
                    entryKeySize = 0L;
                }

                ssTableSize =   Long.BYTES + inMemoryData.values().size() * entryKeySize +
                                Long.BYTES + inMemoryData.values().size() * entryValueSize;

                if (ssTableSize != 0) {
                    long offset = 0L;
                    MemorySegment segmentToWrite = fileChannel.map(
                            FileChannel.MapMode.READ_WRITE,
                            0L,
                            ssTableSize,
                            Arena.global()
                    );
                    segmentToWrite.set(ValueLayout.JAVA_LONG_UNALIGNED, offset, entryKeySize);
                    offset += Long.BYTES + entryKeySize;
                    segmentToWrite.set(ValueLayout.JAVA_LONG_UNALIGNED, offset, entryValueSize);
                    offset += Long.BYTES + entryValueSize;
                }
            }
        }
    }
}
