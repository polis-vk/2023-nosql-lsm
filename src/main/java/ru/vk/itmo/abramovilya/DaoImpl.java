package ru.vk.itmo.abramovilya;

import ru.vk.itmo.BaseEntry;
import ru.vk.itmo.Config;
import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class DaoImpl implements Dao<MemorySegment, Entry<MemorySegment>> {
    private final ConcurrentNavigableMap<MemorySegment, Entry<MemorySegment>> map =
            new ConcurrentSkipListMap<>(DaoImpl::compareMemorySegments);
    private final Path storagePath;
    private final Arena arena = Arena.ofShared();
    private static final String SSTABLE_BASE_NAME = "storage";
    private static final String INDEX_BASE_NAME = "table";
    private final Path metaFilePath;
    private final List<FileChannel> sstableFileChannels = new ArrayList<>();
    private final List<MemorySegment> sstableMappedList = new ArrayList<>();
    private final List<FileChannel> indexFileChannels = new ArrayList<>();
    private final List<MemorySegment> indexMappedList = new ArrayList<>();

    public DaoImpl(Config config) throws IOException {
        storagePath = config.basePath();

        Files.createDirectories(storagePath);
        metaFilePath = storagePath.resolve("meta");
        if (!Files.exists(metaFilePath)) {
            Files.createFile(metaFilePath);
            Files.writeString(metaFilePath, "0", StandardOpenOption.WRITE);
        }

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

    @Override
    public Iterator<Entry<MemorySegment>> get(MemorySegment from, MemorySegment to) {
        return new DaoIterator(getTotalSStables(), from, to, sstableMappedList, indexMappedList, map);
    }

    @Override
    public void upsert(Entry<MemorySegment> entry) {
        map.put(entry.key(), entry);
    }

    @Override
    public Entry<MemorySegment> get(MemorySegment key) {
        var value = map.get(key);
        if (value != null) {
            if (value.value() != null) {
                return value;
            }
            return null;
        }

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

    private Entry<MemorySegment> seekForValueInFile(MemorySegment key, int sstableNum) {
        if (sstableNum >= sstableFileChannels.size()) {
            return null;
        }

        MemorySegment storageMapped = sstableMappedList.get(sstableNum);
        MemorySegment indexMapped = indexMappedList.get(sstableNum);

        int foundIndex = upperBound(key, storageMapped, indexMapped, indexMapped.byteSize());
        MemorySegment foundKey = getKeyFromStorage(storageMapped, indexMapped, foundIndex);
        if (foundKey.mismatch(key) == -1) {
            return getEntryFromIndexFile(storageMapped, indexMapped, foundIndex);
        }
        return null;
    }

    static int upperBound(MemorySegment key, MemorySegment storageMapped, MemorySegment indexMapped, long indexSize) {
        int l = -1;
        int r = indexMapped.get(ValueLayout.JAVA_INT_UNALIGNED, indexSize - Long.BYTES - Integer.BYTES);

        while (r - l > 1) {
            int m = (r + l) / 2;
            MemorySegment ms = getKeyFromStorage(storageMapped, indexMapped, m);

            if (compareMemorySegments(key, ms) > 0) {
                l = m;
            } else {
                r = m;
            }
        }
        return r;
    }

    static MemorySegment getKeyFromStorage(MemorySegment storageMapped, MemorySegment indexMapped, int entryNum) {
        long offsetInStorageFile = indexMapped.get(
                ValueLayout.JAVA_LONG_UNALIGNED,
                (long) (Integer.BYTES + Long.BYTES) * entryNum + Integer.BYTES
        );

        long msSize = storageMapped.get(ValueLayout.JAVA_LONG_UNALIGNED, offsetInStorageFile);
        return storageMapped.asSlice(offsetInStorageFile + Long.BYTES, msSize);
    }

    private Entry<MemorySegment> getEntryFromIndexFile(MemorySegment storageMapped,
                                                       MemorySegment indexMapped,
                                                       int entryNum) {
        long offsetInStorageFile = indexMapped.get(
                ValueLayout.JAVA_LONG_UNALIGNED,
                (long) (Integer.BYTES + Long.BYTES) * entryNum + Integer.BYTES
        );

        long keySize = storageMapped.get(ValueLayout.JAVA_LONG_UNALIGNED, offsetInStorageFile);
        offsetInStorageFile += Long.BYTES;
        offsetInStorageFile += keySize;

        long valueSize = storageMapped.get(ValueLayout.JAVA_LONG_UNALIGNED, offsetInStorageFile);
        offsetInStorageFile += Long.BYTES;
        MemorySegment key = storageMapped.asSlice(offsetInStorageFile - keySize - Long.BYTES, keySize);
        MemorySegment value;
        if (valueSize == -1) {
            value = null;
        } else {
            value = storageMapped.asSlice(offsetInStorageFile, valueSize);
        }
        return new BaseEntry<>(key, value);
    }

    @Override
    public void compact() throws IOException {
        var iterator = get(null, null);
        if (!iterator.hasNext()) {
            return;
        }
        writeIteratorIntoFile(calcComapactedSStableSize(), calcCompactedIndexSize(), iterator);

        int totalSStables = getTotalSStables();
        Files.writeString(metaFilePath, String.valueOf(1));
        for (int i = 0; i < totalSStables; i++) {
            Files.delete(storagePath.resolve(SSTABLE_BASE_NAME + i));
            Files.delete(storagePath.resolve(INDEX_BASE_NAME + i));
        }
        Files.move(storagePath.resolve(SSTABLE_BASE_NAME + totalSStables), storagePath.resolve(SSTABLE_BASE_NAME + 0));
        Files.move(storagePath.resolve(INDEX_BASE_NAME + totalSStables), storagePath.resolve(INDEX_BASE_NAME + 0));
        map.clear();
    }

    private void writeIteratorIntoFile(long size, long size1, Iterator<Entry<MemorySegment>> iterator) throws IOException {
        int totalSStables = getTotalSStables();
        Path sstablePath = storagePath.resolve(SSTABLE_BASE_NAME + totalSStables);
        Path indexPath = storagePath.resolve(INDEX_BASE_NAME + totalSStables);

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
                    storageChannel.map(FileChannel.MapMode.READ_WRITE, 0, size, writeArena);
            MemorySegment mappedIndex =
                    indexChannel.map(FileChannel.MapMode.READ_WRITE, 0, size1, writeArena);

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

    private long calcCompactedIndexSize() {
        var iterator = get(null, null);
        long size = 0;
        while (iterator.hasNext()) {
            iterator.next();
            size += Integer.BYTES + Long.BYTES;
        }
        return size;

    }

    private long calcComapactedSStableSize() {
        var iterator = get(null, null);
        long size = 0;
        while (iterator.hasNext()) {
            var entry = iterator.next();
            size += entry.key().byteSize();
            size += entry.value().byteSize();
            size += 2 * Long.BYTES;
        }
        return size;
    }

    @Override
    public void flush() throws IOException {
        if (!map.isEmpty()) {
            writeMapIntoFile();
            incTotalSStablesAmount();
        }
    }

    private void incTotalSStablesAmount() throws IOException {
        int totalSStables = getTotalSStables();
        Files.writeString(metaFilePath, String.valueOf(totalSStables + 1));
    }

    @Override
    public void close() throws IOException {
        if (arena.scope().isAlive()) {
            arena.close();
        }
        flush();
        for (FileChannel fc : sstableFileChannels) {
            if (fc.isOpen()) fc.close();
        }
        for (FileChannel fc : indexFileChannels) {
            if (fc.isOpen()) fc.close();
        }
    }

    private void writeMapIntoFile() throws IOException {
        if (map.isEmpty()) {
            return;
        }
        writeIteratorIntoFile(calcMapByteSizeInFile(), calcIndexByteSizeInFile(), map.values().iterator());
    }

    private static long writeEntryNumAndStorageOffset(MemorySegment mappedIndex,
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

    private int getTotalSStables() {
        return sstableFileChannels.size();
    }

    private long calcIndexByteSizeInFile() {
        return (long) map.size() * (Integer.BYTES + Long.BYTES);
    }

    private long calcMapByteSizeInFile() {
        long size = 0;
        for (var entry : map.values()) {
            size += 2 * Long.BYTES;
            size += entry.key().byteSize();
            if (entry.value() != null) {
                size += entry.value().byteSize();
            }
        }
        return size;
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

    public static int compareMemorySegments(MemorySegment segment1, MemorySegment segment2) {
        long offset = segment1.mismatch(segment2);
        if (offset == -1) {
            return 0;
        } else if (offset == segment1.byteSize()) {
            return -1;
        } else if (offset == segment2.byteSize()) {
            return 1;
        }
        return Byte.compare(segment1.get(ValueLayout.JAVA_BYTE, offset), segment2.get(ValueLayout.JAVA_BYTE, offset));
    }
}
