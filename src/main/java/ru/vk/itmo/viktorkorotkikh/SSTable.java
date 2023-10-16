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

    private static final long METADATA_SIZE = Long.BYTES;

    private static final long ENTRY_METADATA_SIZE = Long.BYTES;

    private SSTable(Arena arena, MemorySegment mappedSSTableFile) {
        this.arena = arena;
        this.mappedSSTableFile = mappedSSTableFile;
    }

    public static SSTable load(Path basePath) throws IOException {
        Arena arena = null;
        MemorySegment mappedSSTableFile;
        Path filePath = basePath.resolve(FILE_NAME + FILE_EXTENSION);
        if (!Files.exists(filePath)) {
            return null;
        }
        try (FileChannel fileChannel = FileChannel.open(filePath, StandardOpenOption.READ)) {
            arena = Arena.ofShared();
            mappedSSTableFile = fileChannel.map(FileChannel.MapMode.READ_ONLY, 0L, fileChannel.size(), arena);
        } catch (Exception e) {
            if (arena != null) {
                arena.close();
            }
            throw e;
        }
        return new SSTable(arena, mappedSSTableFile);
    }

    public static void save(Collection<Entry<MemorySegment>> entries, Path basePath) throws IOException {
        Path tmpSSTable = basePath.resolve(FILE_NAME + FILE_EXTENSION + TMP_FILE_EXTENSION);

        Files.deleteIfExists(tmpSSTable);
        Files.createFile(tmpSSTable);

        MemorySegment mappedSSTableFile;

        long entriesDataSize = 0;

        for (Entry<MemorySegment> entry : entries) {
            entriesDataSize += getEntrySize(entry);
        }

        long entriesDataOffset = METADATA_SIZE + ENTRY_METADATA_SIZE * entries.size();

        try (Arena arena = Arena.ofConfined();
             FileChannel channel = FileChannel.open(
                     tmpSSTable, StandardOpenOption.READ,
                     StandardOpenOption.WRITE,
                     StandardOpenOption.TRUNCATE_EXISTING
             )
        ) {
            mappedSSTableFile = channel.map(
                    FileChannel.MapMode.READ_WRITE,
                    0L,
                    entriesDataOffset + entriesDataSize,
                    arena
            );


            mappedSSTableFile.set(ValueLayout.JAVA_LONG_UNALIGNED, 0, entries.size());

            long offset = entriesDataOffset;
            long index = 0;
            for (Entry<MemorySegment> entry : entries) {
                // write metadata (offset key size of each entry) at the beginning of the file
                mappedSSTableFile.set(
                        ValueLayout.JAVA_LONG_UNALIGNED,
                        METADATA_SIZE + index * ENTRY_METADATA_SIZE,
                        offset
                );
                offset += writeMemorySegment(mappedSSTableFile, entry.key(), offset);
                offset += writeMemorySegment(mappedSSTableFile, entry.value(), offset);
                index++;
            }

            mappedSSTableFile.force();
            Files.move(tmpSSTable, basePath.resolve(FILE_NAME + FILE_EXTENSION), StandardCopyOption.ATOMIC_MOVE);
        }
    }

    private static long getEntrySize(Entry<MemorySegment> entry) {
        return Long.BYTES + entry.key().byteSize() + Long.BYTES + entry.value().byteSize();
    }

    private static long writeMemorySegment(
            MemorySegment ssTableMemorySegment,
            MemorySegment memorySegmentToWrite,
            long offset
    ) {
        long memorySegmentToWriteSize = memorySegmentToWrite.byteSize();
        // write memorySegment size and memorySegment
        ssTableMemorySegment.set(ValueLayout.JAVA_LONG_UNALIGNED, offset, memorySegmentToWriteSize);
        MemorySegment.copy(
                memorySegmentToWrite,
                0,
                ssTableMemorySegment,
                offset + Long.BYTES,
                memorySegmentToWrite.byteSize()
        );
        return Long.BYTES + memorySegmentToWriteSize;
    }

    public Entry<MemorySegment> get(MemorySegment key) {
        long entryOffset = getEntryOffset(key);
        if (entryOffset == -1) {
            return null;
        }

        long keySize = mappedSSTableFile.get(ValueLayout.JAVA_LONG_UNALIGNED, entryOffset);
        MemorySegment savedKey = mappedSSTableFile.asSlice(entryOffset + Long.BYTES, keySize);

        long valueOffset = entryOffset + Long.BYTES + keySize;
        long valueSize = mappedSSTableFile.get(ValueLayout.JAVA_LONG_UNALIGNED, valueOffset);
        return new BaseEntry<>(savedKey, mappedSSTableFile.asSlice(valueOffset + Long.BYTES, valueSize));
    }

    private long getEntryOffset(MemorySegment key) {
        // binary search
        long entriesSize = mappedSSTableFile.get(ValueLayout.JAVA_LONG_UNALIGNED, 0);
        long left = 0;
        long right = entriesSize - 1;
        while (left <= right) {
            long mid = (right + left) / 2;
            long keySizeOffset = mappedSSTableFile.get(
                    ValueLayout.JAVA_LONG_UNALIGNED,
                    METADATA_SIZE + mid * ENTRY_METADATA_SIZE
            );
            long keySize = mappedSSTableFile.get(ValueLayout.JAVA_LONG_UNALIGNED, keySizeOffset);
            long keyOffset = keySizeOffset + Long.BYTES;
            int keyComparison = MemorySegmentComparator.INSTANCE.compare(
                    mappedSSTableFile, keyOffset,
                    keyOffset + keySize,
                    key
            );
            if (keyComparison < 0) {
                left = mid + 1;
            } else if (keyComparison > 0) {
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
