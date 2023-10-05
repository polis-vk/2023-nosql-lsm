package ru.vk.itmo.gamzatgadzhimagomedov;

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

    private static final String TABLE_NAME = "sstable.db";

    private final Path ssTablePath;
    private final Comparator<MemorySegment> comparator;

    public SSTable(Config config, Comparator<MemorySegment> comparator) {
        ssTablePath = config.basePath().resolve(TABLE_NAME);
        this.comparator = comparator;
    }

    public Entry<MemorySegment> get(MemorySegment key) {
        if (!Files.exists(ssTablePath)) {
            return null;
        }

        try (FileChannel fileChannel = FileChannel.open(ssTablePath, StandardOpenOption.READ)) {
            MemorySegment data = fileChannel.map(FileChannel.MapMode.READ_ONLY, 0, fileChannel.size(), Arena.global());

            long offset = 0;
            while (offset < data.byteSize()) {
                long keySize = data.get(ValueLayout.JAVA_LONG_UNALIGNED, offset);
                offset += Long.BYTES;

                MemorySegment keySegment = data.asSlice(offset, keySize);
                offset += keySize;

                long valueSize = data.get(ValueLayout.JAVA_LONG_UNALIGNED, offset);
                offset += Long.BYTES;

                if (comparator.compare(keySegment, key) == 0) {
                    MemorySegment valueSegment = data.asSlice(offset, valueSize);

                    return new BaseEntry<>(keySegment, valueSegment);
                }

                offset += valueSize;
            }

            return null;
        } catch (IOException e) {
            return null;
        }
    }

    public void flush(Collection<Entry<MemorySegment>> memTable) throws IOException {
        if (memTable.isEmpty()) return;

        long memTableSize = 0;
        for (Entry<MemorySegment> entry : memTable) {
            memTableSize += Long.BYTES * 2 + entry.key().byteSize() + entry.value().byteSize();
        }

        try (FileChannel ssTable = FileChannel.open(ssTablePath,
                StandardOpenOption.CREATE,
                StandardOpenOption.READ,
                StandardOpenOption.WRITE)
        ) {
            MemorySegment segment = ssTable.map(FileChannel.MapMode.READ_WRITE, 0, memTableSize, Arena.global());

            long offset = 0;
            for (Entry<MemorySegment> entry : memTable) {
                segment.set(ValueLayout.JAVA_LONG_UNALIGNED, offset, entry.key().byteSize());
                offset += Long.BYTES;

                segment.asSlice(offset).copyFrom(entry.key());
                offset += entry.key().byteSize();

                segment.set(ValueLayout.JAVA_LONG_UNALIGNED, offset, entry.value().byteSize());
                offset += Long.BYTES;

                segment.asSlice(offset).copyFrom(entry.value());
                offset += entry.value().byteSize();
            }
        }
    }
}
