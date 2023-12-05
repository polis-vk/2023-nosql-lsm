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
import java.util.NavigableMap;

class Storage implements Closeable {
    private static final String COMPACTED_SUFFIX = "_compacted";
    private static final String COMPACTING_SUFFIX = "_compacting";
    private static final String SSTABLE_BASE_NAME = "storage";
    private static final String INDEX_BASE_NAME = "index";
    public static final String META_FILE_BASE_NAME = "meta";
    public static final int BYTES_TO_STORE_ENTRY_ELEMENT_SIZE = Long.BYTES;
    public static final int BYTES_TO_STORE_ENTRY_SIZE = 2 * BYTES_TO_STORE_ENTRY_ELEMENT_SIZE;
    public static final int BYTES_TO_STORE_INDEX_KEY = Integer.BYTES;
    public static final long INDEX_ENTRY_SIZE = BYTES_TO_STORE_INDEX_KEY + BYTES_TO_STORE_ENTRY_ELEMENT_SIZE;
    private final Path storagePath;
    private final Path metaFilePath;
    private final List<FileChannel> sstableFileChannels = new ArrayList<>();
    private final List<MemorySegment> sstableMappedList = new ArrayList<>();
    private final List<FileChannel> indexFileChannels = new ArrayList<>();
    private final List<MemorySegment> indexMappedList = new ArrayList<>();

    Storage(Config config, Arena arena) throws IOException {
        storagePath = config.basePath();

        Files.createDirectories(storagePath);
        metaFilePath = storagePath.resolve(META_FILE_BASE_NAME);
        if (!Files.exists(metaFilePath)) {
            Files.createFile(metaFilePath);

            int totalSStables = 0;
            Files.writeString(metaFilePath, String.valueOf(totalSStables), StandardOpenOption.WRITE);
        }

        // Restore consistent state if db was dropped during compaction
        if (Files.exists(storagePath.resolve(SSTABLE_BASE_NAME + COMPACTED_SUFFIX))
                || Files.exists(storagePath.resolve(INDEX_BASE_NAME + COMPACTED_SUFFIX))) {
            finishCompact();
        }

        // Delete artifacts from unsuccessful compaction
        Files.deleteIfExists(storagePath.resolve(SSTABLE_BASE_NAME + COMPACTING_SUFFIX));
        Files.deleteIfExists(storagePath.resolve(INDEX_BASE_NAME + COMPACTING_SUFFIX));

        int totalSSTables = Integer.parseInt(Files.readString(metaFilePath));
        for (int sstableNum = 0; sstableNum < totalSSTables; sstableNum++) {
            Path sstablePath = storagePath.resolve(SSTABLE_BASE_NAME + sstableNum);
            Path indexPath = storagePath.resolve(INDEX_BASE_NAME + sstableNum);

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

    final int getTotalSStables() {
        return sstableFileChannels.size();
    }

    private Entry<MemorySegment> seekForValueInFile(MemorySegment key, int sstableNum) {
        if (sstableNum >= sstableFileChannels.size()) {
            return null;
        }

        MemorySegment storageMapped = sstableMappedList.get(sstableNum);
        MemorySegment indexMapped = indexMappedList.get(sstableNum);

        int foundIndex = upperBound(key, storageMapped, indexMapped, indexMapped.byteSize());
        long keyStorageOffset = getKeyStorageOffset(indexMapped, foundIndex);
        long foundKeySize = storageMapped.get(ValueLayout.JAVA_LONG_UNALIGNED, keyStorageOffset);
        keyStorageOffset += BYTES_TO_STORE_ENTRY_ELEMENT_SIZE;

        if (MemorySegment.mismatch(key,
                0,
                key.byteSize(),
                storageMapped,
                keyStorageOffset,
                keyStorageOffset + foundKeySize) == -1) {
            return getEntryFromIndexFile(storageMapped, indexMapped, foundIndex);
        }
        return null;
    }

    static long getKeyStorageOffset(MemorySegment indexMapped, int entryNum) {
        return indexMapped.get(
                ValueLayout.JAVA_LONG_UNALIGNED,
                INDEX_ENTRY_SIZE * entryNum + BYTES_TO_STORE_INDEX_KEY
        );
    }

    private Entry<MemorySegment> getEntryFromIndexFile(MemorySegment sstableMapped,
                                                       MemorySegment indexMapped,
                                                       int entryNum) {
        long offsetInStorageFile = indexMapped.get(
                ValueLayout.JAVA_LONG_UNALIGNED,
                INDEX_ENTRY_SIZE * entryNum + BYTES_TO_STORE_INDEX_KEY
        );

        long keySize = sstableMapped.get(ValueLayout.JAVA_LONG_UNALIGNED, offsetInStorageFile);
        offsetInStorageFile += BYTES_TO_STORE_ENTRY_ELEMENT_SIZE;
        offsetInStorageFile += keySize;

        long valueSize = sstableMapped.get(ValueLayout.JAVA_LONG_UNALIGNED, offsetInStorageFile);
        offsetInStorageFile += BYTES_TO_STORE_ENTRY_ELEMENT_SIZE;
        MemorySegment key = sstableMapped.asSlice(
                offsetInStorageFile - keySize - BYTES_TO_STORE_ENTRY_ELEMENT_SIZE, keySize);
        MemorySegment value;
        if (valueSize == -1) {
            value = null;
        } else {
            value = sstableMapped.asSlice(offsetInStorageFile, valueSize);
        }
        return new BaseEntry<>(key, value);
    }

    void writeMapIntoFile(long sstableSize, long indexSize, NavigableMap<MemorySegment, Entry<MemorySegment>> map)
            throws IOException {

        int totalSStables = getTotalSStables();
        Path sstablePath = storagePath.resolve(Storage.SSTABLE_BASE_NAME + totalSStables);
        Path indexPath = storagePath.resolve(Storage.INDEX_BASE_NAME + totalSStables);
        StorageFileWriter.writeMapIntoFile(sstableSize, indexSize, map, sstablePath, indexPath);
    }

    private Entry<Long> calcCompactedSStableIndexSize(Iterator<Entry<MemorySegment>> iterator) {
        long storageSize = 0;
        long indexSize = 0;
        while (iterator.hasNext()) {
            Entry<MemorySegment> entry = iterator.next();
            storageSize += entry.key().byteSize() + entry.value().byteSize() + BYTES_TO_STORE_ENTRY_SIZE;
            indexSize += INDEX_ENTRY_SIZE;
        }
        return new BaseEntry<>(storageSize, indexSize);
    }

    void incTotalSStablesAmount() throws IOException {
        int totalSStables = getTotalSStables();
        Files.writeString(metaFilePath, String.valueOf(totalSStables + 1));
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

    void compact(Iterator<Entry<MemorySegment>> iterator1, Iterator<Entry<MemorySegment>> iterator2)
            throws IOException {
        Entry<Long> storageIndexSize = calcCompactedSStableIndexSize(iterator1);
        Path compactingSStablePath = storagePath.resolve(SSTABLE_BASE_NAME + COMPACTING_SUFFIX);
        Path compactingIndexPath = storagePath.resolve(INDEX_BASE_NAME + COMPACTING_SUFFIX);
        StorageFileWriter.writeIteratorIntoFile(storageIndexSize.key(),
                storageIndexSize.value(),
                iterator2,
                compactingSStablePath,
                compactingIndexPath);

        // Move to ensure that compacting completed successfully
        Path compactedSStablePath = storagePath.resolve(SSTABLE_BASE_NAME + COMPACTED_SUFFIX);
        Path compactedIndexPath = storagePath.resolve(INDEX_BASE_NAME + COMPACTED_SUFFIX);
        Files.move(compactingSStablePath, compactedSStablePath, StandardCopyOption.ATOMIC_MOVE);
        Files.move(compactingIndexPath, compactedIndexPath, StandardCopyOption.ATOMIC_MOVE);

        finishCompact();
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

    long findOffsetInIndex(MemorySegment from, MemorySegment to, int fileNum) {
        long readOffset = 0;
        MemorySegment storageMapped = sstableMappedList.get(fileNum);
        MemorySegment indexMapped = indexMappedList.get(fileNum);

        if (from == null && to == null) {
            return BYTES_TO_STORE_INDEX_KEY;
        } else if (from == null) {
            long firstKeySize = storageMapped.get(ValueLayout.JAVA_LONG_UNALIGNED, readOffset);
            readOffset += BYTES_TO_STORE_ENTRY_ELEMENT_SIZE;
            MemorySegment firstKey = storageMapped.asSlice(readOffset, firstKeySize);
            if (DaoImpl.compareMemorySegments(firstKey, to) >= 0) {
                return -1;
            }
            return BYTES_TO_STORE_INDEX_KEY;
        } else {
            int foundIndex = upperBound(from, storageMapped, indexMapped, indexMapped.byteSize());
            long keyStorageOffset = getKeyStorageOffset(indexMapped, foundIndex);
            long keySize = storageMapped.get(ValueLayout.JAVA_LONG_UNALIGNED, keyStorageOffset);
            keyStorageOffset += BYTES_TO_STORE_ENTRY_ELEMENT_SIZE;

            if (DaoImpl.compareMemorySegmentsUsingOffset(from, storageMapped, keyStorageOffset, keySize) > 0
                    || (to != null && DaoImpl.compareMemorySegmentsUsingOffset(
                    to, storageMapped, keyStorageOffset, keySize) <= 0)) {
                return -1;
            }
            return (long) foundIndex * INDEX_ENTRY_SIZE + BYTES_TO_STORE_INDEX_KEY;
        }
    }

    private static int upperBound(MemorySegment key,
                                  MemorySegment storageMapped,
                                  MemorySegment indexMapped,
                                  long indexSize) {
        int l = -1;
        int r = indexMapped.get(ValueLayout.JAVA_INT_UNALIGNED, indexSize - INDEX_ENTRY_SIZE);

        while (r - l > 1) {
            int m = (r + l) / 2;
            long keyStorageOffset = getKeyStorageOffset(indexMapped, m);
            long keySize = storageMapped.get(ValueLayout.JAVA_LONG_UNALIGNED, keyStorageOffset);
            keyStorageOffset += BYTES_TO_STORE_ENTRY_ELEMENT_SIZE;

            if (DaoImpl.compareMemorySegmentsUsingOffset(key, storageMapped, keyStorageOffset, keySize) > 0) {
                l = m;
            } else {
                r = m;
            }
        }
        return r;
    }
}
