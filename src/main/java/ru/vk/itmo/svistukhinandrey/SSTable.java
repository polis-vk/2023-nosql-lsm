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

    private static final int BLOCK_SIZE = Byte.SIZE * 8;
    private static final long DATA_SIZE = ValueLayout.JAVA_BYTE.byteSize();
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
            pos += BLOCK_SIZE;
            if (memorySegmentComparator.compare(key, foundKey) == 0) { // compare with mismatch
                byte valueSize = data.get(ValueLayout.JAVA_BYTE, pos++);
                return new BaseEntry<>(foundKey, data.asSlice(pos, valueSize));
            }
            pos += BLOCK_SIZE + DATA_SIZE;
        }

        return null;
    }

    public void save(Collection<Entry<MemorySegment>> data) throws IOException {
        if (data.isEmpty()) {
            return;
        }

        MemorySegment indexesMemorySegment;
        Files.deleteIfExists(ssTablePath);
        Files.createFile(ssTablePath);

        long dataBlockSize = (BLOCK_SIZE + DATA_SIZE) * 2L * data.size();

        try (FileChannel indexesFileChannel = FileChannel.open(ssTablePath, StandardOpenOption.READ, StandardOpenOption.WRITE)) {
            indexesMemorySegment = indexesFileChannel.map(FileChannel.MapMode.READ_WRITE, 0, dataBlockSize, Arena.global());

            long pos = 0;
            for (Entry<MemorySegment> entry : data) {
                indexesMemorySegment.set(ValueLayout.JAVA_BYTE, pos, (byte) entry.key().byteSize());
                pos += DATA_SIZE;
                indexesMemorySegment.asSlice(pos).copyFrom(entry.key());
                pos += BLOCK_SIZE;
                indexesMemorySegment.set(ValueLayout.JAVA_BYTE, pos, (byte) entry.value().byteSize());
                pos += DATA_SIZE;
                indexesMemorySegment.asSlice(pos).copyFrom(entry.value());
                pos += BLOCK_SIZE;
            }
        }
    }
}
