package ru.vk.itmo.svistukhinandrey;

import ru.vk.itmo.BaseEntry;
import ru.vk.itmo.Config;
import ru.vk.itmo.Entry;

import java.io.Closeable;
import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Collection;

public class SSTable implements Closeable {

    private static final long BLOCK_SIZE = ValueLayout.JAVA_LONG_UNALIGNED.byteSize();
    private static final String SS_TABLE_FILENAME = "sstable.db";
    private final Path ssTablePath;
    private final MemorySegment data;
    private final Arena arena;
    private final MemorySegmentComparator memorySegmentComparator;

    public SSTable(Config config) throws IOException {
        arena = Arena.ofConfined();
        ssTablePath = config.basePath().resolve(SS_TABLE_FILENAME);
        if (!Files.exists(ssTablePath)) {
            data = null;
            memorySegmentComparator = null;
            return;
        }

        memorySegmentComparator = new MemorySegmentComparator();

        try (FileChannel dataFileChannel = FileChannel.open(ssTablePath)) {
            data = dataFileChannel.map(FileChannel.MapMode.READ_ONLY, 0, dataFileChannel.size(), arena);
        }
    }

    public Entry<MemorySegment> get(MemorySegment key) {
        if (data == null) {
            return null;
        }

        long pos = 0L;
        while (pos < data.byteSize()) {
            long keySize = data.get(ValueLayout.JAVA_LONG_UNALIGNED, pos);
            pos += BLOCK_SIZE;
            MemorySegment foundKey = data.asSlice(pos, keySize);
            pos += foundKey.byteSize();

            long valueSize = data.get(ValueLayout.JAVA_LONG_UNALIGNED, pos);
            pos += BLOCK_SIZE;
            if (memorySegmentComparator.compare(key, foundKey) == 0) {
                return new BaseEntry<>(foundKey, data.asSlice(pos, valueSize));
            }
            pos += valueSize;
        }

        return null;
    }

    public void save(Collection<Entry<MemorySegment>> entries) throws IOException {
        if (entries.isEmpty()) {
            return;
        }

        MemorySegment ssTableMemorySegment;
        Files.deleteIfExists(ssTablePath);
        Files.createFile(ssTablePath);

        long dataBlockSize = 0L;

        for (Entry<MemorySegment> entry : entries) {
            dataBlockSize += BLOCK_SIZE + entry.key().byteSize() + BLOCK_SIZE + entry.value().byteSize();
        }

        try (FileChannel ssTable = FileChannel.open(ssTablePath, StandardOpenOption.READ, StandardOpenOption.WRITE)) {
            ssTableMemorySegment = ssTable.map(FileChannel.MapMode.READ_WRITE, 0, dataBlockSize, arena);

            long pos = 0;
            for (Entry<MemorySegment> entry : entries) {
                ssTableMemorySegment.set(ValueLayout.JAVA_LONG_UNALIGNED, pos, entry.key().byteSize());
                pos += BLOCK_SIZE;

                MemorySegment.copy(entry.key(), 0, ssTableMemorySegment, pos, entry.key().byteSize());
                pos += entry.key().byteSize();

                long valueSize = entry.value().byteSize();
                ssTableMemorySegment.set(ValueLayout.JAVA_LONG_UNALIGNED, pos, valueSize);
                pos += BLOCK_SIZE;

                MemorySegment.copy(entry.value(), 0, ssTableMemorySegment, pos, valueSize);
                pos += entry.value().byteSize();
            }
        }
    }

    @Override
    public void close() {
        arena.close();
    }
}
