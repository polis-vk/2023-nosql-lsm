package ru.vk.itmo.abramovilya;

import ru.vk.itmo.Entry;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Iterator;
import java.util.NavigableMap;

final class StorageFileWriter {

    public static final ValueLayout.OfInt ENTRY_NUMBER_LAYOUT = ValueLayout.JAVA_INT_UNALIGNED;
    public static final ValueLayout.OfLong MEMORY_SEGMENT_SIZE_LAYOUT = ValueLayout.JAVA_LONG_UNALIGNED;

    private StorageFileWriter() {
    }

    static void writeIteratorIntoFile(long storageSize,
                                      long indexSize,
                                      Iterator<Entry<MemorySegment>> iterator,
                                      Path sstablePath,
                                      Path indexPath) throws IOException {
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

            MemorySegment mappedStorage =
                    storageChannel.map(FileChannel.MapMode.READ_WRITE, 0, storageSize, writeArena);
            MemorySegment mappedIndex =
                    indexChannel.map(FileChannel.MapMode.READ_WRITE, 0, indexSize, writeArena);

            int entryNum = 0;
            while (iterator.hasNext()) {
                var entry = iterator.next();
                indexWriteOffset =
                        writeEntryNumAndStorageOffset(mappedIndex, indexWriteOffset, entryNum, storageWriteOffset);
                entryNum++;

                storageWriteOffset = writeMemorySegment(entry.key(), mappedStorage, storageWriteOffset);
                storageWriteOffset = writeMemorySegment(entry.value(), mappedStorage, storageWriteOffset);
            }
            mappedStorage.force();
            mappedIndex.force();
        }
    }

    // writeMapIntoFile and writeIteratorInto file are pretty much the same,
    // but I can't use writeIteratorIntoFile here due to optimization purposes:
    // I have to write sstable and index separately
    // I can't use writeMapIntoFile's code in the method above either,
    // because it will slow down the execution due to the need of creating iterator twice
    // And it also won't give any speed boost,
    // because I would still be in need to find iterator.next() entry in another file
    static void writeMapIntoFile(long sstableSize,
                                        long indexSize,
                                        NavigableMap<MemorySegment, Entry<MemorySegment>> map,
                                        Path sstablePath,
                                        Path indexPath) throws IOException {
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
                indexWriteOffset = writeEntryNumAndStorageOffset(
                        mappedIndex,
                        indexWriteOffset,
                        entryNum,
                        storageWriteOffset
                );
                entryNum++;

                storageWriteOffset += Storage.BYTES_TO_STORE_ENTRY_SIZE;
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
                storageWriteOffset = writeMemorySegment(entry.key(), mappedStorage, storageWriteOffset);
                storageWriteOffset = writeMemorySegment(entry.value(), mappedStorage, storageWriteOffset);
            }
            mappedStorage.force();
        }
    }

    static long writeEntryNumAndStorageOffset(MemorySegment mappedIndex,
                                              long indexWriteOffset,
                                              int entryNum,
                                              long storageWriteOffset) {
        long offset = indexWriteOffset;
        mappedIndex.set(ENTRY_NUMBER_LAYOUT, offset, entryNum);
        offset += Storage.BYTES_TO_STORE_INDEX_KEY;
        mappedIndex.set(MEMORY_SEGMENT_SIZE_LAYOUT, offset, storageWriteOffset);
        offset += Storage.BYTES_TO_STORE_ENTRY_ELEMENT_SIZE;
        return offset;
    }

    // Every memorySegment in file has the following structure:
    // 8 bytes - size, <size> bytes - value
    // If memorySegment has the size of -1 byte, then it means its value is DELETED
    static long writeMemorySegment(MemorySegment memorySegment, MemorySegment mapped, long writeOffset) {
        long offset = writeOffset;
        if (memorySegment == null) {
            mapped.set(MEMORY_SEGMENT_SIZE_LAYOUT, offset, -1);
            offset += Storage.BYTES_TO_STORE_ENTRY_ELEMENT_SIZE;
        } else {
            long msSize = memorySegment.byteSize();
            mapped.set(MEMORY_SEGMENT_SIZE_LAYOUT, offset, msSize);
            offset += Storage.BYTES_TO_STORE_ENTRY_ELEMENT_SIZE;
            MemorySegment.copy(memorySegment, 0, mapped, offset, msSize);
            offset += msSize;
        }
        return offset;
    }

}
