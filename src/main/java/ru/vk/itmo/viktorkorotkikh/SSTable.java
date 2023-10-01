package ru.vk.itmo.viktorkorotkikh;

import ru.vk.itmo.BaseEntry;
import ru.vk.itmo.Entry;

import java.io.Closeable;
import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.Collection;

public final class SSTable implements Closeable {

    private final Arena arena;

    private final MemorySegment mappedSSTableFile;

    private static final String FILE_NAME = "sstable";

    private static final String FILE_EXTENSION = ".db";

    private static final String TMP_FILE_EXTENSION = ".tmp";

    private SSTable(Arena arena, MemorySegment mappedSSTableFile) {
        this.arena = arena;
        this.mappedSSTableFile = mappedSSTableFile;
    }

    public static SSTable load(Path basePath) throws IOException {
        Arena arena;
        MemorySegment mappedSSTableFile;
        Path filePath = basePath.resolve(FILE_NAME + FILE_EXTENSION);
        if (!Files.exists(filePath)) {
            return null;
        }
        try (FileChannel fileChannel = FileChannel.open(filePath, StandardOpenOption.READ)) {
            arena = Arena.ofShared();
            mappedSSTableFile = fileChannel.map(FileChannel.MapMode.READ_ONLY, 0L, fileChannel.size(), arena);
        }
        return new SSTable(arena, mappedSSTableFile);
    }

    public static void save(Collection<Entry<MemorySegment>> entries, Path basePath) throws IOException {
        Path tmpSSTable = basePath.resolve(FILE_NAME + FILE_EXTENSION + TMP_FILE_EXTENSION);

        Files.deleteIfExists(tmpSSTable);
        Files.createFile(tmpSSTable);

        Arena arena;
        MemorySegment mappedSSTableFile;

        long size = 0;

        for (Entry<MemorySegment> entry : entries) {
            size += getEntrySize(entry);
        }

        try (FileChannel channel = FileChannel.open(tmpSSTable, StandardOpenOption.READ, StandardOpenOption.WRITE)) {
            arena = Arena.ofConfined();
            mappedSSTableFile = channel.map(FileChannel.MapMode.READ_WRITE, 0L, size, arena);
        }

        long offset = 0;
        for (Entry<MemorySegment> entry : entries) {
            offset += writeMemorySegment(mappedSSTableFile, entry.key(), offset);
            offset += writeMemorySegment(mappedSSTableFile, entry.value(), offset);
        }

        mappedSSTableFile.force();
        Files.move(tmpSSTable, basePath.resolve(FILE_NAME + FILE_EXTENSION), StandardCopyOption.ATOMIC_MOVE);
        arena.close();
    }

    private static long getEntrySize(Entry<MemorySegment> entry) {
        return Byte.SIZE + entry.key().byteSize() + Byte.SIZE + entry.value().byteSize();
    }

    private static long writeMemorySegment(
            MemorySegment ssTableMemorySegment,
            MemorySegment memorySegmentToWrite,
            long offset
    ) {
        long memorySegmentToWriteSize = memorySegmentToWrite.byteSize();
        ssTableMemorySegment.set(ValueLayout.JAVA_LONG_UNALIGNED, offset, memorySegmentToWriteSize);
        ssTableMemorySegment.asSlice(offset + Byte.SIZE, memorySegmentToWriteSize).copyFrom(memorySegmentToWrite);
        return Byte.SIZE + memorySegmentToWriteSize;
    }

    public Entry<MemorySegment> get(MemorySegment key) {
        long offset = 0;
        while (offset < this.mappedSSTableFile.byteSize()) {
            long keySize = mappedSSTableFile.get(ValueLayout.JAVA_LONG_UNALIGNED, offset);
            MemorySegment savedKey = mappedSSTableFile.asSlice(offset + Byte.SIZE, keySize);

            boolean keyWasFound = MemorySegmentComparator.INSTANCE.compare(key, savedKey) == 0;

            long valueOffset = offset + Byte.SIZE + keySize;
            long valueSize = mappedSSTableFile.get(ValueLayout.JAVA_LONG_UNALIGNED, valueOffset);

            if (keyWasFound) {
                return new BaseEntry<>(savedKey, mappedSSTableFile.asSlice(valueOffset + Byte.SIZE, valueSize));
            }
            offset = valueOffset + valueSize + Byte.SIZE;
        }
        return null;
    }

    @Override
    public void close() {
        arena.close();
    }
}
