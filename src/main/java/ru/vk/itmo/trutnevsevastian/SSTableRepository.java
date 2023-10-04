package ru.vk.itmo.trutnevsevastian;

import ru.vk.itmo.Entry;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Comparator;
import java.util.Set;
import java.util.SortedMap;

public class SSTableRepository {

    private final Comparator<MemorySegment> comparator;

    SSTableRepository(Comparator<MemorySegment> comparator) {
        this.comparator = comparator;
    }

    void save(SortedMap<MemorySegment, Entry<MemorySegment>> memTable, Path filePath) throws IOException {
        var chanelOptions = Set.of(
                StandardOpenOption.CREATE,
                StandardOpenOption.WRITE,
                StandardOpenOption.READ
        );
        try (var channel = FileChannel.open(filePath, chanelOptions)) {

            var memTableSnapshot = memTable.values();

            long memTableSize = 0;
            for (var entry : memTableSnapshot) {
                memTableSize += Long.BYTES * 2 + entry.key().byteSize() + entry.value().byteSize();
            }

            MemorySegment writeSegment = channel.map(FileChannel.MapMode.READ_WRITE, 0, memTableSize, Arena.global());
            int offset = 0;
            for (var entry : memTable.values()) {
                long keySize = entry.key().byteSize();
                writeSegment.set(ValueLayout.JAVA_LONG_UNALIGNED, offset, keySize);
                offset += Long.BYTES;

                writeSegment.asSlice(offset).copyFrom(entry.key());
                offset += keySize;

                long valueSize = entry.value().byteSize();
                writeSegment.set(ValueLayout.JAVA_LONG_UNALIGNED, offset, valueSize);
                offset += Long.BYTES;

                writeSegment.asSlice(offset).copyFrom(entry.value());
                offset += valueSize;
            }
        }
    }

    SSTable read(Path filePath) throws IOException {
        var chanelOptions = Set.of(StandardOpenOption.READ);
        try (var channel = FileChannel.open(filePath, chanelOptions)) {
            MemorySegment readSegment = channel.map(FileChannel.MapMode.READ_ONLY, 0, channel.size(), Arena.global());
            return new SSTable(readSegment, comparator);
        }
    }

}
