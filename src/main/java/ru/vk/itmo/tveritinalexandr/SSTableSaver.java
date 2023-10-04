package ru.vk.itmo.tveritinalexandr;

import ru.vk.itmo.Entry;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.SortedMap;

import static java.lang.foreign.ValueLayout.JAVA_LONG_UNALIGNED;

public class SSTableSaver {
    private final Path SSTableFilePath;
    private final SortedMap<MemorySegment, Entry<MemorySegment>> memTable;
    private long offset;

    public SSTableSaver(Path SSTableFilePath, SortedMap<MemorySegment, Entry<MemorySegment>> memTable) {
        this.SSTableFilePath = SSTableFilePath;
        this.memTable = memTable;
    }

    public void save() throws IOException {
        try (FileChannel channel = FileChannel.open(SSTableFilePath,
                StandardOpenOption.READ,
                StandardOpenOption.WRITE,
                StandardOpenOption.CREATE);
             Arena arena = Arena.ofConfined()
        ) {
            MemorySegment memorySegment = channel.map(FileChannel.MapMode.READ_WRITE, 0, calcFileSize(memTable), arena);
            offset = 0;
            for (var entry : memTable.values()) {
                fillMemorySegment(entry.key(), memorySegment);
                fillMemorySegment(entry.value(), memorySegment);
            }
            memorySegment.load();
        }
    }

    private long calcFileSize(SortedMap<MemorySegment, Entry<MemorySegment>> inMemoryDB) {
        long size = 0;
        for (var entry : inMemoryDB.values()) {
            size += 2 * Long.BYTES + entry.key().byteSize() + entry.value().byteSize();
        }
        return size;
    }

    private void fillMemorySegment(MemorySegment memorySegment, MemorySegment mapped) {
        long size = memorySegment.byteSize();
        mapped.set(JAVA_LONG_UNALIGNED, offset, size);
        offset += Long.BYTES;
        MemorySegment.copy(memorySegment, 0, mapped, offset, size);
        offset += size;
    }
}
