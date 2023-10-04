package ru.vk.itmo.reshetnikovaleksei;

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
import java.util.Collection;
import java.util.Comparator;

public class SSTable {
    private static final String SS_TABLE_NAME = "ss_table.db";

    private final Path ssTablePath;
    private final Comparator<MemorySegment> comparator;

    public SSTable(Config config, Comparator<MemorySegment> comparator) {
        this.ssTablePath = config.basePath().resolve(SS_TABLE_NAME);
        this.comparator = comparator;
    }

    public Entry<MemorySegment> get(MemorySegment key) throws IOException {
        if (Files.notExists(ssTablePath)) {
            return null;
        }

        try (FileChannel ssTableChannel = FileChannel.open(ssTablePath, StandardOpenOption.READ)) {
            long offset = 0;
            MemorySegment ssTableMemorySegment = ssTableChannel.map(
                    FileChannel.MapMode.READ_ONLY, offset, ssTableChannel.size(), Arena.ofConfined());

            while (ssTableMemorySegment.byteSize() > offset) {
                long keySize = ssTableMemorySegment.get(ValueLayout.JAVA_LONG_UNALIGNED, offset);
                offset += Long.BYTES;

                long valueSize = ssTableMemorySegment.get(ValueLayout.JAVA_LONG_UNALIGNED, offset);
                offset += Long.BYTES;

                MemorySegment ssTableKey = ssTableMemorySegment.asSlice(offset, keySize);
                offset += keySize;

                if (comparator.compare(ssTableKey, key) == 0) {
                    MemorySegment ssTableValue = ssTableMemorySegment.asSlice(offset, valueSize);
                    return new BaseEntry<>(ssTableKey, ssTableValue);
                }

                offset += valueSize;
            }
        }

        return null;
    }

    public void save(Collection<Entry<MemorySegment>> entries) throws IOException {
        try (FileChannel ssTableChannel = FileChannel.open(
                ssTablePath,
                StandardOpenOption.READ,
                StandardOpenOption.CREATE,
                StandardOpenOption.WRITE
        )) {
            long size = 0;
            for (Entry<MemorySegment> entry : entries) {
                size += entry.key().byteSize() + entry.value().byteSize() + 2 * Long.BYTES;
            }

            long offset = 0;
            MemorySegment ssTableMemorySegment = ssTableChannel.map(
                    FileChannel.MapMode.READ_WRITE, offset, size, Arena.ofConfined());
            for (Entry<MemorySegment> entry : entries) {
                offset = setMemorySegment(offset, entry.key(), ssTableMemorySegment);
                offset = setMemorySegment(offset, entry.value(), ssTableMemorySegment);
            }
        }
    }

    private long setMemorySegment(long offset, MemorySegment entryMemorySegment, MemorySegment ssTableMemorySegment) {
        long newOffset = offset;
        long entryByteSize = entryMemorySegment.byteSize();

        ssTableMemorySegment.set(ValueLayout.JAVA_LONG_UNALIGNED, newOffset, entryByteSize);
        newOffset += Long.BYTES;

        ssTableMemorySegment.asSlice(newOffset, entryByteSize).copyFrom(entryMemorySegment);
        newOffset += entryByteSize;

        return newOffset;
    }
}
