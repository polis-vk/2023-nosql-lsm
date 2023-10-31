package ru.vk.itmo.abramovilya;

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
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class Storage implements Closeable {
    private static final String COMPACTED_SUFFIX = "_compacted";
    private static final String COMPACTING_SUFFIX = "_compacting";
    final Path storagePath;
    static final String SSTABLE_BASE_NAME = "storage";
    static final String INDEX_BASE_NAME = "table";
    final Path metaFilePath;
    final List<FileChannel> sstableFileChannels = new ArrayList<>();
    final List<MemorySegment> sstableMappedList = new ArrayList<>();
    final List<FileChannel> indexFileChannels = new ArrayList<>();
    final List<MemorySegment> indexMappedList = new ArrayList<>();

    public Storage(Config config, Arena arena) throws IOException {
        storagePath = config.basePath();

        Files.createDirectories(storagePath);
        metaFilePath = storagePath.resolve("meta");
        if (!Files.exists(metaFilePath)) {
            Files.createFile(metaFilePath);
            Files.writeString(metaFilePath, "0", StandardOpenOption.WRITE);
        }

        // Restore consistent state if db was dropped during compaction
        if (Files.exists(storagePath.resolve(Storage.SSTABLE_BASE_NAME + COMPACTED_SUFFIX)) ||
                Files.exists(storagePath.resolve(Storage.INDEX_BASE_NAME + COMPACTED_SUFFIX))) {
            finishCompact();
        }

        // Delete artifacts from unsuccessful compaction
        Files.deleteIfExists(storagePath.resolve(Storage.SSTABLE_BASE_NAME + COMPACTING_SUFFIX));
        Files.deleteIfExists(storagePath.resolve(Storage.SSTABLE_BASE_NAME + COMPACTING_SUFFIX));

        int totalSSTables = Integer.parseInt(Files.readString(metaFilePath));
        for (int sstableNum = 0; sstableNum < totalSSTables; sstableNum++) {
            Path sstablePath = storagePath.resolve(Storage.SSTABLE_BASE_NAME + sstableNum);
            Path indexPath = storagePath.resolve(Storage.INDEX_BASE_NAME + sstableNum);

            FileChannel sstableFileChannel = FileChannel.open(sstablePath, StandardOpenOption.READ);
            sstableFileChannels.add(sstableFileChannel);
            MemorySegment sstableMapped =
                    sstableFileChannel.map(FileChannel.MapMode.READ_ONLY, 0, Files.size(sstablePath), arena);
            sstableMappedList.add(sstableMapped);

            FileChannel indexFileChannel = FileChannel.open(indexPath, StandardOpenOption.READ);
            indexFileChannels.add(indexFileChannel);
            MemorySegment indexMapped =
                    indexFileChannel.map(FileChannel.MapMode.READ_ONLY, 0, Files.size(indexPath), arena);
            indexMappedList.add(indexMapped);
        }
    }

    Entry<MemorySegment> get(MemorySegment key) {
        int totalSStables = getTotalSStables();
        for (int sstableNum = totalSStables; sstableNum >= 0; sstableNum--) {
            var foundEntry = seekForValueInFile(key, sstableNum);
            if (foundEntry != null) {
                if (foundEntry.value() != null) {
                    return foundEntry;
                }
                return null;
            }
        }
        return null;
    }

    int getTotalSStables() {
        return sstableFileChannels.size();
    }

    private Entry<MemorySegment> seekForValueInFile(MemorySegment key, int sstableNum) {
        if (sstableNum >= sstableFileChannels.size()) {
            return null;
        }

        MemorySegment storageMapped = sstableMappedList.get(sstableNum);
        MemorySegment indexMapped = indexMappedList.get(sstableNum);

        int foundIndex = upperBound(key, storageMapped, indexMapped, indexMapped.byteSize());
        MemorySegment foundKey = getKeyFromSStable(storageMapped, indexMapped, foundIndex);
        if (foundKey.mismatch(key) == -1) {
            return getEntryFromIndexFile(storageMapped, indexMapped, foundIndex);
        }
        return null;
    }

    static MemorySegment getKeyFromSStable(MemorySegment sstableMapped, MemorySegment indexMapped, int entryNum) {
        long offsetInStorageFile = indexMapped.get(
                ValueLayout.JAVA_LONG_UNALIGNED,
                (long) (Integer.BYTES + Long.BYTES) * entryNum + Integer.BYTES
        );

        long msSize = sstableMapped.get(ValueLayout.JAVA_LONG_UNALIGNED, offsetInStorageFile);
        return sstableMapped.asSlice(offsetInStorageFile + Long.BYTES, msSize);
    }

    private Entry<MemorySegment> getEntryFromIndexFile(MemorySegment sstableMapped,
                                                       MemorySegment indexMapped,
                                                       int entryNum) {
        long offsetInStorageFile = indexMapped.get(
                ValueLayout.JAVA_LONG_UNALIGNED,
                (long) (Integer.BYTES + Long.BYTES) * entryNum + Integer.BYTES
        );

        long keySize = sstableMapped.get(ValueLayout.JAVA_LONG_UNALIGNED, offsetInStorageFile);
        offsetInStorageFile += Long.BYTES;
        offsetInStorageFile += keySize;

        long valueSize = sstableMapped.get(ValueLayout.JAVA_LONG_UNALIGNED, offsetInStorageFile);
        offsetInStorageFile += Long.BYTES;
        MemorySegment key = sstableMapped.asSlice(offsetInStorageFile - keySize - Long.BYTES, keySize);
        MemorySegment value;
        if (valueSize == -1) {
            value = null;
        } else {
            value = sstableMapped.asSlice(offsetInStorageFile, valueSize);
        }
        return new BaseEntry<>(key, value);
    }

    private void finishCompact() throws IOException {
        int totalSStables = getTotalSStables();
        for (int i = 0; i < totalSStables; i++) {
            Files.deleteIfExists(storagePath.resolve(SSTABLE_BASE_NAME + i));
            Files.deleteIfExists(storagePath.resolve(INDEX_BASE_NAME + i));
        }

        Files.writeString(metaFilePath, String.valueOf(1));
        Path compactedSStablePath = storagePath.resolve(SSTABLE_BASE_NAME + COMPACTED_SUFFIX);
        if (Files.exists(compactedSStablePath)) {
            Files.move(compactedSStablePath,
                    storagePath.resolve(SSTABLE_BASE_NAME + 0),
                    StandardCopyOption.ATOMIC_MOVE,
                    StandardCopyOption.REPLACE_EXISTING);
        }
        Path compactedIndexPath = storagePath.resolve(INDEX_BASE_NAME + COMPACTED_SUFFIX);

        if (Files.exists(compactedIndexPath)) {
            Files.move(compactedIndexPath,
                    storagePath.resolve(INDEX_BASE_NAME + 0),
                    StandardCopyOption.ATOMIC_MOVE,
                    StandardCopyOption.REPLACE_EXISTING);
        }
    }

    void writeIteratorIntoFile(long storageSize,
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
            mappedStorage.load();
            mappedIndex.load();
        }
    }

    private SStableSizeIndexSize calcCompactedSStableIndexSize(Iterator<Entry<MemorySegment>> iterator) {
        long storageSize = 0;
        long indexSize = 0;
        while (iterator.hasNext()) {
            Entry<MemorySegment> entry = iterator.next();
            storageSize += entry.key().byteSize() + entry.value().byteSize() + 2 * Long.BYTES;
            indexSize += Integer.BYTES + Long.BYTES;
        }
        return new SStableSizeIndexSize(storageSize, indexSize);
    }

    void incTotalSStablesAmount() throws IOException {
        int totalSStables = getTotalSStables();
        Files.writeString(metaFilePath, String.valueOf(totalSStables + 1));
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

    // Every memorySegment in file has the following structure:
    // 8 bytes - size, <size> bytes - value
    // If memorySegment has the size of -1 byte, then it means its value is DELETED

    private static long writeMemorySegment(MemorySegment memorySegment, MemorySegment mapped, long writeOffset) {
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

    @Override
    public void close() throws IOException {
        for (FileChannel fc : sstableFileChannels) {
            if (fc.isOpen()) fc.close();
        }
        for (FileChannel fc : indexFileChannels) {
            if (fc.isOpen()) fc.close();
        }
    }

    public MemorySegment mappedSStable(int i) {
        return sstableMappedList.get(i);
    }

    public MemorySegment mappedIndex(int i) {
        return indexMappedList.get(i);
    }

    // For some reason, Gradle issues an "Unused Variable" warning if this class is private
    record SStableSizeIndexSize(long sstableSize, long indexSize) {
    }

    void compact(Iterator<Entry<MemorySegment>> iterator1, Iterator<Entry<MemorySegment>> iterator2) throws IOException {
        SStableSizeIndexSize storageIndexSize = calcCompactedSStableIndexSize(iterator1);
        Path compactingSStablePath = storagePath.resolve(Storage.SSTABLE_BASE_NAME + COMPACTING_SUFFIX);
        Path compactingIndexPath = storagePath.resolve(Storage.INDEX_BASE_NAME + COMPACTING_SUFFIX);
        writeIteratorIntoFile(storageIndexSize.sstableSize(),
                storageIndexSize.indexSize(),
                iterator2,
                compactingSStablePath,
                compactingIndexPath);

        // Move to ensure that compacting completed successfully
        Path compactedSStablePath = storagePath.resolve(Storage.SSTABLE_BASE_NAME + COMPACTED_SUFFIX);
        Path compactedIndexPath = storagePath.resolve(Storage.INDEX_BASE_NAME + COMPACTED_SUFFIX);
        Files.move(compactingSStablePath, compactedSStablePath, StandardCopyOption.ATOMIC_MOVE);
        Files.move(compactingIndexPath, compactedIndexPath, StandardCopyOption.ATOMIC_MOVE);

        finishCompact();
    }

    long findOffsetInIndex(MemorySegment from, MemorySegment to, int fileNum) {
        long readOffset = 0;
        MemorySegment storageMapped = sstableMappedList.get(fileNum);
        MemorySegment indexMapped = indexMappedList.get(fileNum);

        if (from == null && to == null) {
            return Integer.BYTES;
        } else if (from == null) {
            long firstKeySize = storageMapped.get(ValueLayout.JAVA_LONG_UNALIGNED, readOffset);
            readOffset += Long.BYTES;
            MemorySegment firstKey = storageMapped.asSlice(readOffset, firstKeySize);
            if (DaoImpl.compareMemorySegments(firstKey, to) >= 0) {
                return -1;
            }
            return Integer.BYTES;
        } else {
            int foundIndex = Storage.upperBound(from, storageMapped, indexMapped, indexMapped.byteSize());
            MemorySegment foundMemorySegment = Storage.getKeyFromSStable(storageMapped, indexMapped, foundIndex);
            if (DaoImpl.compareMemorySegments(foundMemorySegment, from) < 0
                    || (to != null && DaoImpl.compareMemorySegments(foundMemorySegment, to) >= 0)) {
                return -1;
            }
            return (long) foundIndex * (Integer.BYTES + Long.BYTES) + Integer.BYTES;
        }
    }

    private static int upperBound(MemorySegment key, MemorySegment storageMapped, MemorySegment indexMapped, long indexSize) {
        int l = -1;
        int r = indexMapped.get(ValueLayout.JAVA_INT_UNALIGNED, indexSize - Long.BYTES - Integer.BYTES);

        while (r - l > 1) {
            int m = (r + l) / 2;
            MemorySegment ms = Storage.getKeyFromSStable(storageMapped, indexMapped, m);

            if (DaoImpl.compareMemorySegments(key, ms) > 0) {
                l = m;
            } else {
                r = m;
            }
        }
        return r;
    }
}
