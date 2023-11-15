package ru.vk.itmo.abramovilya;

import ru.vk.itmo.Entry;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.NavigableMap;

final class StorageWriter {
    private StorageWriter() {
    }

    static long writeEntryNumAndStorageOffset(MemorySegment mappedIndex,
                                              long indexWriteOffset,
                                              int entryNum,
                                              long storageWriteOffset) {
        long offset = indexWriteOffset;
        mappedIndex.set(ValueLayout.JAVA_INT_UNALIGNED, offset, entryNum);
        offset += Integer.BYTES;
        mappedIndex.set(ValueLayout.JAVA_LONG_UNALIGNED, offset, storageWriteOffset);
        offset += Long.BYTES;
        return offset;
    }

    static long writeMemorySegment(MemorySegment memorySegment, MemorySegment mapped, long writeOffset) {
        long offset = writeOffset;
        if (memorySegment == null) {
            mapped.set(ValueLayout.JAVA_LONG_UNALIGNED, offset, -1);
            offset += Long.BYTES;
        } else {
            long msSize = memorySegment.byteSize();
            mapped.set(ValueLayout.JAVA_LONG_UNALIGNED, offset, msSize);
            offset += Long.BYTES;
            MemorySegment.copy(memorySegment, 0, mapped, offset, msSize);
            offset += msSize;
        }
        return offset;
    }

    // SSTable: |keySize: 8 bytes|key|valueSize: 8 bytes (size == -1 means value is deleted)|value|
    // Index: |entryNum: 4 bytes|storageKeyOffset: 8 bytes|
    static void writeSStableAndIndex(Path sstablePath,
                                     long sstableSize,
                                     Path indexPath,
                                     long indexSize,
                                     NavigableMap<MemorySegment, Entry<MemorySegment>> map) throws IOException {
        long storageWriteOffset = 0;
        long indexWriteOffset = 0;
        try (var storageChannel = FileChannel.open(sstablePath,
                StandardOpenOption.READ,
                StandardOpenOption.WRITE,
                StandardOpenOption.CREATE);

             var indexChannel = FileChannel.open(indexPath,
                     StandardOpenOption.READ,
                     StandardOpenOption.WRITE,
                     StandardOpenOption.CREATE);

             var writeArena = Arena.ofConfined()) {
            MemorySegment mappedIndex =
                    indexChannel.map(FileChannel.MapMode.READ_WRITE, 0, indexSize, writeArena);

            int entryNum = 0;
            for (var entry : map.values()) {
                indexWriteOffset = StorageWriter.writeEntryNumAndStorageOffset(
                        mappedIndex,
                        indexWriteOffset,
                        entryNum,
                        storageWriteOffset
                );
                entryNum++;

                storageWriteOffset += 2 * Long.BYTES;
                storageWriteOffset += entry.key().byteSize();
                if (entry.value() != null) {
                    storageWriteOffset += entry.value().byteSize();
                }
            }
            mappedIndex.force();

            MemorySegment mappedStorage =
                    storageChannel.map(FileChannel.MapMode.READ_WRITE, 0, sstableSize, writeArena);
            storageWriteOffset = 0;
            for (var entry : map.values()) {
                storageWriteOffset = StorageWriter.writeMemorySegment(entry.key(), mappedStorage, storageWriteOffset);
                storageWriteOffset = StorageWriter.writeMemorySegment(entry.value(), mappedStorage, storageWriteOffset);
            }
            mappedStorage.force();
        }
    }
}
