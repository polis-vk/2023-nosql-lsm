package ru.vk.itmo.svistukhinandrey;

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

public class SSTable {

    private static final long KEY_BLOCK_SIZE = ValueLayout.JAVA_BYTE.byteSize() * 64L;
    private static final long KEY_MAX_LENGTH = ValueLayout.JAVA_BYTE.byteSize();
    private static final long VALUE_MAX_LENGTH = ValueLayout.JAVA_SHORT_UNALIGNED.byteSize();
    private static final String SS_TABLE_FILENAME = "sstable.db";
    private final Path ssTablePath;
    private final MemorySegment data;
    private final MemorySegmentComparator memorySegmentComparator;

    public SSTable(Config config) throws IOException {
        ssTablePath = config.basePath().resolve(SS_TABLE_FILENAME);
        if (!Files.exists(ssTablePath)) {
            data = null;
            memorySegmentComparator = null;
            return;
        }

        memorySegmentComparator = new MemorySegmentComparator();

        try (FileChannel dataFileChannel = FileChannel.open(ssTablePath)) {
            data = dataFileChannel.map(FileChannel.MapMode.READ_ONLY, 0, dataFileChannel.size(), Arena.global());
        }
    }

    public Entry<MemorySegment> get(MemorySegment key) {
        if (data == null) {
            return null;
        }

        long pos = 0L;
        while (pos < data.byteSize()) {
            byte keySize = data.get(ValueLayout.JAVA_BYTE, pos++);
            MemorySegment foundKey = data.asSlice(pos, keySize);
            pos += KEY_BLOCK_SIZE;
            short valueSize = data.get(ValueLayout.JAVA_SHORT_UNALIGNED, pos);
            pos += VALUE_MAX_LENGTH;
            long fragmentation = ((valueSize + KEY_BLOCK_SIZE - 1) / KEY_BLOCK_SIZE) * KEY_BLOCK_SIZE;
            if (memorySegmentComparator.compare(key, foundKey) == 0) {
                return new BaseEntry<>(foundKey, data.asSlice(pos, valueSize));
            }
            pos += fragmentation;
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
            long fragmentation = ((entry.value().byteSize() + KEY_BLOCK_SIZE - 1) / KEY_BLOCK_SIZE) * KEY_BLOCK_SIZE;
            dataBlockSize += KEY_MAX_LENGTH + KEY_BLOCK_SIZE + VALUE_MAX_LENGTH + fragmentation;
        }

        try (FileChannel ssTable = FileChannel.open(ssTablePath, StandardOpenOption.READ, StandardOpenOption.WRITE)) {
            ssTableMemorySegment = ssTable.map(FileChannel.MapMode.READ_WRITE, 0, dataBlockSize, Arena.global());

            long pos = 0;
            for (Entry<MemorySegment> entry : entries) {
                ssTableMemorySegment.set(ValueLayout.JAVA_BYTE, pos, (byte) entry.key().byteSize());
                pos += KEY_MAX_LENGTH;
                ssTableMemorySegment.asSlice(pos).copyFrom(entry.key());
                pos += KEY_BLOCK_SIZE;

                long valueSize = entry.value().byteSize();
                ssTableMemorySegment.set(ValueLayout.JAVA_SHORT_UNALIGNED, pos, (short) valueSize);
                pos += VALUE_MAX_LENGTH;

                ssTableMemorySegment.asSlice(pos).copyFrom(entry.value());
                long fragmentation = ((valueSize + KEY_BLOCK_SIZE - 1) / KEY_BLOCK_SIZE) * KEY_BLOCK_SIZE;
                pos += fragmentation;
            }
        }
    }
}
