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

    private static final long METADATA_SIZE = Byte.SIZE;

    private static final long ENTRY_METADATA_SIZE = Byte.SIZE;

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

        long entriesDataSize = 0;

        for (Entry<MemorySegment> entry : entries) {
            entriesDataSize += getEntrySize(entry);
        }

        // оставляем место в начале файла под метаданные:
        // кол-во записей, offset'ы, по которым получаем размер ключа
        long entriesDataOffset = METADATA_SIZE + ENTRY_METADATA_SIZE * entries.size();

        try (FileChannel channel = FileChannel.open(tmpSSTable, StandardOpenOption.READ, StandardOpenOption.WRITE)) {
            arena = Arena.ofConfined();
            mappedSSTableFile = channel.map(
                    FileChannel.MapMode.READ_WRITE,
                    0L,
                    entriesDataOffset + entriesDataSize,
                    arena
            );
        }

        mappedSSTableFile.set(ValueLayout.JAVA_LONG_UNALIGNED, 0, entries.size()); // кол-во записей в sstable

        long offset = entriesDataOffset;
        long index = 0;
        for (Entry<MemorySegment> entry : entries) {
            // в начало файла записываем offset размера ключа каждой записи
            mappedSSTableFile.set(ValueLayout.JAVA_LONG_UNALIGNED, METADATA_SIZE + index * ENTRY_METADATA_SIZE, offset);
            // записываем ключ и значение
            offset += writeMemorySegment(mappedSSTableFile, entry.key(), offset);
            offset += writeMemorySegment(mappedSSTableFile, entry.value(), offset);
            index++;
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
        // сначала размер memorySegment, потом сам memorySegment
        ssTableMemorySegment.set(ValueLayout.JAVA_LONG_UNALIGNED, offset, memorySegmentToWriteSize);
        ssTableMemorySegment.asSlice(offset + Byte.SIZE, memorySegmentToWriteSize).copyFrom(memorySegmentToWrite);
        return Byte.SIZE + memorySegmentToWriteSize;
    }

    public Entry<MemorySegment> get(MemorySegment key) {
        long entryOffset = getEntryOffset(key);
        if (entryOffset == -1) {
            return null;
        }

        long keySize = mappedSSTableFile.get(ValueLayout.JAVA_LONG_UNALIGNED, entryOffset);
        MemorySegment savedKey = mappedSSTableFile.asSlice(entryOffset + Byte.SIZE, keySize);

        long valueOffset = entryOffset + Byte.SIZE + keySize;
        long valueSize = mappedSSTableFile.get(ValueLayout.JAVA_LONG_UNALIGNED, valueOffset);
        return new BaseEntry<>(savedKey, mappedSSTableFile.asSlice(valueOffset + Byte.SIZE, valueSize));
    }

    private long getEntryOffset(MemorySegment key) {
        // бинарный поиск по offset ключей, которые записаны в начале файла
        long entriesSize = mappedSSTableFile.get(ValueLayout.JAVA_LONG_UNALIGNED, 0);
        long left = 0;
        long right = entriesSize - 1;
        while (left <= right) {
            long mid = (right + left) / 2;
            // offset по которому достаём размер ключа записи
            long keySizeOffset = mappedSSTableFile.get(
                    ValueLayout.JAVA_LONG_UNALIGNED,
                    METADATA_SIZE + mid * ENTRY_METADATA_SIZE
            );
            long keySize = mappedSSTableFile.get(ValueLayout.JAVA_LONG_UNALIGNED, keySizeOffset);
            MemorySegment keyFromSSTable = mappedSSTableFile.asSlice(keySizeOffset + Byte.SIZE, keySize);
            int keyComparison = MemorySegmentComparator.INSTANCE.compare(key, keyFromSSTable);
            if (keyComparison > 0) {
                left = mid + 1;
            } else if (keyComparison < 0) {
                right = mid - 1;
            } else {
                return keySizeOffset;
            }
        }
        return -1;
    }

    @Override
    public void close() {
        arena.close();
    }
}
