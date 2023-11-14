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
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

class Storage implements Closeable {
    private static final String COMPACTED_SUFFIX = "_compacted";
    private static final String COMPACTING_SUFFIX = "_compacting";
    private static final String SSTABLE_BASE_NAME = "storage";
    private static final String INDEX_BASE_NAME = "index";
    private final Path storagePath;
    private final Path metaFilePath;

    // TODO: Писать это в meta файл
    private final Path compactedTablesAmountPath;
    private List<MemorySegment> sstableMappedList = new ArrayList<>();
    private List<MemorySegment> indexMappedList = new ArrayList<>();
    private final Arena arena = Arena.ofAuto();
    // Блокировка используется для поддержания консистентного количества текущих sstable
    private final ReadWriteLock sstablesAmountRWLock = new ReentrantReadWriteLock();

    Storage(Config config) throws IOException {
        storagePath = config.basePath();

        Files.createDirectories(storagePath);
        compactedTablesAmountPath = storagePath.resolve("cmpctd");
        metaFilePath = storagePath.resolve("meta");
        if (!Files.exists(metaFilePath)) {
            Files.createFile(metaFilePath);
            Files.writeString(metaFilePath, "0", StandardOpenOption.WRITE);
        }

        // Restore consistent state if db was dropped during compaction
        if (Files.exists(compactedTablesAmountPath)) {
            finishCompact(Integer.parseInt(Files.readString(compactedTablesAmountPath)));
        } else {
            int totalSSTables = Integer.parseInt(Files.readString(metaFilePath));
            fillFileRepresentationLists(totalSSTables);
        }

        // Delete artifacts from unsuccessful compaction
        Files.deleteIfExists(storagePath.resolve(SSTABLE_BASE_NAME + COMPACTING_SUFFIX));
        Files.deleteIfExists(storagePath.resolve(SSTABLE_BASE_NAME + COMPACTING_SUFFIX));
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
        sstablesAmountRWLock.readLock().lock();
        try {
            return sstableMappedList.size();
        } finally {
            sstablesAmountRWLock.readLock().unlock();
        }
    }

    private Entry<MemorySegment> seekForValueInFile(MemorySegment key, int sstableNum) {
        sstablesAmountRWLock.readLock().lock();
        try {
            if (sstableNum >= sstableMappedList.size()) {
                return null;
            }

            MemorySegment storageMapped = sstableMappedList.get(sstableNum);
            MemorySegment indexMapped = indexMappedList.get(sstableNum);

            int foundIndex = upperBound(key, storageMapped, indexMapped, indexMapped.byteSize());
            long keyStorageOffset = getKeyStorageOffset(indexMapped, foundIndex);
            long foundKeySize = storageMapped.get(ValueLayout.JAVA_LONG_UNALIGNED, keyStorageOffset);
            keyStorageOffset += Long.BYTES;

            if (MemorySegment.mismatch(key,
                    0,
                    key.byteSize(),
                    storageMapped,
                    keyStorageOffset,
                    keyStorageOffset + foundKeySize) == -1) {

                return getEntryFromIndexFile(storageMapped, indexMapped, foundIndex);
            }
            return null;
        } finally {
            sstablesAmountRWLock.readLock().unlock();
        }
    }

    static long getKeyStorageOffset(MemorySegment indexMapped, int entryNum) {
        return indexMapped.get(
                ValueLayout.JAVA_LONG_UNALIGNED,
                (long) (Integer.BYTES + Long.BYTES) * entryNum + Integer.BYTES
        );
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

    void writeMapIntoFile(long sstableSize, long indexSize, NavigableMap<MemorySegment, Entry<MemorySegment>> map)
            throws IOException {
        // Блокировка нужна чтобы totalSStables было верным на момент создания файла
        sstablesAmountRWLock.readLock().lock();
        try {
            int totalSStables = getTotalSStables();
            Path sstablePath = storagePath.resolve(Storage.SSTABLE_BASE_NAME + totalSStables);
            Path indexPath = storagePath.resolve(Storage.INDEX_BASE_NAME + totalSStables);
            StorageFileWriter.writeMapIntoFile(sstableSize, indexSize, map, sstablePath, indexPath);
        } finally {
            sstablesAmountRWLock.readLock().unlock();
        }
    }

    private Entry<Long> calcCompactedSStableIndexSize(Iterator<Entry<MemorySegment>> iterator) {
        long storageSize = 0;
        long indexSize = 0;
        while (iterator.hasNext()) {
            Entry<MemorySegment> entry = iterator.next();
            storageSize += entry.key().byteSize() + entry.value().byteSize() + 2 * Long.BYTES;
            indexSize += Integer.BYTES + Long.BYTES;
        }
        return new BaseEntry<>(storageSize, indexSize);
    }

    void incTotalSStablesAmount() throws IOException {
        sstablesAmountRWLock.writeLock().lock();
        try {
            int totalSStables = sstableMappedList.size();
            Files.writeString(metaFilePath, String.valueOf(totalSStables + 1));

            Path sstablePath = storagePath.resolve(SSTABLE_BASE_NAME + totalSStables);
            Path indexPath = storagePath.resolve(INDEX_BASE_NAME + totalSStables);

            MemorySegment sstableMapped;
            try (FileChannel sstableFileChannel = FileChannel.open(sstablePath, StandardOpenOption.READ)) {
                sstableMapped =
                        sstableFileChannel.map(FileChannel.MapMode.READ_ONLY, 0, Files.size(sstablePath), arena);
            }
            sstableMappedList.add(sstableMapped);

            MemorySegment indexMapped;
            try (FileChannel indexFileChannel = FileChannel.open(indexPath, StandardOpenOption.READ)) {
                indexMapped = indexFileChannel.map(FileChannel.MapMode.READ_ONLY, 0, Files.size(indexPath), arena);
            }
            indexMappedList.add(indexMapped);
        } finally {
            sstablesAmountRWLock.writeLock().unlock();
        }
    }

    @Override
    public void close() {
//        if (arena.scope().isAlive()) {
//            arena.close();
//        }
    }

    public MemorySegment mappedSStable(int i) {
        return sstableMappedList.get(i);
    }

    public MemorySegment mappedIndex(int i) {
        return indexMappedList.get(i);
    }

    void compact(Iterator<Entry<MemorySegment>> iterator1, Iterator<Entry<MemorySegment>> iterator2, int sstablesToCompact)
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

        if (!Files.exists(compactedTablesAmountPath)) {
            Files.createFile(compactedTablesAmountPath);
        }
        Files.writeString(compactedTablesAmountPath, String.valueOf(sstablesToCompact), StandardOpenOption.WRITE);

        finishCompact(sstablesToCompact);
    }

    private void finishCompact(int compactedSStablesAmount) throws IOException {
        sstablesAmountRWLock.writeLock().lock();
        try {
            for (int i = 0; i < compactedSStablesAmount; i++) {
                Files.deleteIfExists(storagePath.resolve(SSTABLE_BASE_NAME + i));
                Files.deleteIfExists(storagePath.resolve(INDEX_BASE_NAME + i));
            }

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

            int totalSStables = Integer.parseInt(Files.readString(metaFilePath));

            sstableMappedList = new ArrayList<>();
            indexMappedList = new ArrayList<>();

            for (int i = compactedSStablesAmount; i < totalSStables; i++) {
                Path oldSStablePath = storagePath.resolve(SSTABLE_BASE_NAME + i);
                Path newSStablePath = storagePath.resolve(SSTABLE_BASE_NAME + convertOldFileNumToNew(i, compactedSStablesAmount));
                Path oldIndexPath = storagePath.resolve(INDEX_BASE_NAME + i);
                Path newIndexPath = storagePath.resolve(INDEX_BASE_NAME + convertOldFileNumToNew(i, compactedSStablesAmount));

                if (Files.exists(oldSStablePath)) {
                    Files.move(oldSStablePath, newSStablePath, StandardCopyOption.ATOMIC_MOVE);
                }
                if (Files.exists(oldIndexPath)) {
                    Files.move(oldIndexPath, newIndexPath, StandardCopyOption.ATOMIC_MOVE);
                }
            }

            int newTotalSStables = convertOldFileNumToNew(totalSStables, compactedSStablesAmount);
            fillFileRepresentationLists(newTotalSStables);

            Files.writeString(metaFilePath, String.valueOf(newTotalSStables));
            Files.deleteIfExists(compactedTablesAmountPath);
        } finally {
            sstablesAmountRWLock.writeLock().unlock();
        }
    }

    private void fillFileRepresentationLists(int newTotalSStables) throws IOException {
        for (int sstableNum = 0; sstableNum < newTotalSStables; sstableNum++) {
            Path sstablePath = storagePath.resolve(SSTABLE_BASE_NAME + sstableNum);
            Path indexPath = storagePath.resolve(INDEX_BASE_NAME + sstableNum);

            MemorySegment sstableMapped;
            try (FileChannel sstableFileChannel = FileChannel.open(sstablePath, StandardOpenOption.READ)) {
                sstableMapped =
                        sstableFileChannel.map(FileChannel.MapMode.READ_ONLY, 0, Files.size(sstablePath), this.arena);
            }
            sstableMappedList.add(sstableMapped);

            MemorySegment indexMapped;
            try (FileChannel indexFileChannel = FileChannel.open(indexPath, StandardOpenOption.READ)) {
                indexMapped =
                        indexFileChannel.map(FileChannel.MapMode.READ_ONLY, 0, Files.size(indexPath), this.arena);
            }
            indexMappedList.add(indexMapped);
        }
    }

    private static int convertOldFileNumToNew(int oldNum, int compactedSStablesNum) {
        return oldNum - compactedSStablesNum + 1;
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
            int foundIndex = upperBound(from, storageMapped, indexMapped, indexMapped.byteSize());
            long keyStorageOffset = getKeyStorageOffset(indexMapped, foundIndex);
            long keySize = storageMapped.get(ValueLayout.JAVA_LONG_UNALIGNED, keyStorageOffset);
            keyStorageOffset += Long.BYTES;

            if (DaoImpl.compareMemorySegmentsUsingOffset(from, storageMapped, keyStorageOffset, keySize) > 0
                    || (to != null && DaoImpl.compareMemorySegmentsUsingOffset(
                    to, storageMapped, keyStorageOffset, keySize) <= 0)) {
                return -1;
            }
            return (long) foundIndex * (Integer.BYTES + Long.BYTES) + Integer.BYTES;
        }
    }

    private static int upperBound(MemorySegment key,
                                  MemorySegment storageMapped,
                                  MemorySegment indexMapped,
                                  long indexSize) {
        int l = -1;
        int r = indexMapped.get(ValueLayout.JAVA_INT_UNALIGNED, indexSize - Long.BYTES - Integer.BYTES);

        while (r - l > 1) {
            int m = (r + l) / 2;
            long keyStorageOffset = getKeyStorageOffset(indexMapped, m);
            long keySize = storageMapped.get(ValueLayout.JAVA_LONG_UNALIGNED, keyStorageOffset);
            keyStorageOffset += Long.BYTES;

            if (DaoImpl.compareMemorySegmentsUsingOffset(key, storageMapped, keyStorageOffset, keySize) > 0) {
                l = m;
            } else {
                r = m;
            }
        }
        return r;
    }
}
