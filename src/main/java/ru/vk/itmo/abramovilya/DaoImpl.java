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
    }

    static int upperBound(MemorySegment key, MemorySegment storageMapped, MemorySegment indexMapped, long indexSize) {
        int l = -1;
        int r = indexMapped.get(ValueLayout.JAVA_INT_UNALIGNED, indexSize - Long.BYTES - Integer.BYTES);

        while (r - l > 1) {
            int m = (r + l) / 2;
            long keyStorageOffset = getKeyStorageOffset(indexMapped, m);
            long keySize = storageMapped.get(ValueLayout.JAVA_LONG_UNALIGNED, keyStorageOffset);
            keyStorageOffset += Long.BYTES;

            if (compareMemorySegmentsUsingOffset(key, storageMapped, keyStorageOffset, keySize) > 0) {
                l = m;
            } else {
                r = m;
            }
        }
        return r;
    }

    static long getKeyStorageOffset(MemorySegment indexMapped, int entryNum) {
        return indexMapped.get(
                ValueLayout.JAVA_LONG_UNALIGNED,
                (long) (Integer.BYTES + Long.BYTES) * entryNum + Integer.BYTES
        );
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
    public void flush() throws IOException {
        writeMapIntoFile();
        if (!map.isEmpty()) incTotalSStablesAmount();
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

        int currSStableNum = getTotalSStables();
        Path sstablePath = storagePath.resolve(SSTABLE_BASE_NAME + currSStableNum);
        Path indexPath = storagePath.resolve(INDEX_BASE_NAME + currSStableNum);

        StorageWriter.writeSStableAndIndex(sstablePath,
                calcMapByteSizeInFile(),
                indexPath,
                calcIndexByteSizeInFile(),
                map);
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

    public static int compareMemorySegments(MemorySegment segment1, MemorySegment segment2) {
        long mismatch = segment1.mismatch(segment2);
        if (mismatch == -1) {
            return 0;
        } else if (mismatch == segment1.byteSize()) {
            return -1;
        } else if (mismatch == segment2.byteSize()) {
            return 1;
        }
        return Byte.compare(segment1.get(ValueLayout.JAVA_BYTE, mismatch),
                segment2.get(ValueLayout.JAVA_BYTE, mismatch));
    }

    public static int compareMemorySegmentsUsingOffset(MemorySegment segment1,
                                                       MemorySegment segment2,
                                                       long segment2Offset,
                                                       long segment2Size) {
        long mismatch = MemorySegment.mismatch(segment1,
                0,
                segment1.byteSize(),
                segment2,
                segment2Offset,
                segment2Offset + segment2Size);
        if (mismatch == -1) {
            return 0;
        } else if (mismatch == segment1.byteSize()) {
            return -1;
        } else if (mismatch == segment2Size) {
            return 1;
        }
        return Byte.compare(segment1.get(ValueLayout.JAVA_BYTE, mismatch),
                segment2.get(ValueLayout.JAVA_BYTE, segment2Offset + mismatch));

    }
}
