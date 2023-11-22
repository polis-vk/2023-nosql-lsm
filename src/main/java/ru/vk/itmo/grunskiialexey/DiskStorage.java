package ru.vk.itmo.grunskiialexey;

import ru.vk.itmo.grunskiialexey.model.ActualFilesInterval;

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
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

public class DiskStorage {
    private static final String NAME_TMP_INDEX_FILE = "index.tmp";
    private static final String NAME_INDEX_FILE = "index.idx";
    private final List<MemorySegment> segmentList;
    private final Path storagePath;
    private final Arena arena;

    public DiskStorage(Path storagePath, Arena arena) {
        this.segmentList = new ArrayList<>();
        this.storagePath = storagePath;
        this.arena = arena;
    }

    public List<MemorySegment> loadOrRecover(
            AtomicLong firstFileNumber,
            AtomicLong lastFileNumber
    ) throws IOException {
        Path indexTmp = storagePath.resolve(NAME_TMP_INDEX_FILE);
        Path indexFile = storagePath.resolve(NAME_INDEX_FILE);

        if (Files.exists(indexTmp)) {
            Files.move(indexTmp, indexFile, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
        }

        final ActualFilesInterval interval = getActualFilesInterval(indexFile, arena);
        firstFileNumber.set(interval.left());
        lastFileNumber.set(interval.right());

        for (long i = interval.left(); i < interval.right(); ++i) {
            Path file = storagePath.resolve(Long.toString(i));
            try (FileChannel fileChannel = FileChannel.open(file, StandardOpenOption.READ, StandardOpenOption.WRITE)) {
                MemorySegment segment = fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, Files.size(file), arena);
                segmentList.add(segment);
            }
        }

        return segmentList;
    }

    public synchronized void addNewList(long fileNumber) throws IOException {
        Path file = storagePath.resolve(Long.toString(fileNumber));
        try (FileChannel fileChannel = FileChannel.open(file, StandardOpenOption.READ, StandardOpenOption.WRITE)) {
            MemorySegment segment = fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, Files.size(file), arena);
            segmentList.add(segment);
        }
    }

    static ActualFilesInterval getActualFilesInterval(final Path indexFile, final Arena arena) throws IOException {
        long left;
        long right;
        try (FileChannel fileChannel = FileChannel.open(indexFile,
                StandardOpenOption.READ,
                StandardOpenOption.WRITE,
                StandardOpenOption.CREATE
        )) {
            MemorySegment fileSegment = fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, 2 * Long.BYTES, arena);
            left = fileSegment.getAtIndex(ValueLayout.JAVA_LONG_UNALIGNED, 0);
            right = fileSegment.getAtIndex(ValueLayout.JAVA_LONG_UNALIGNED, 1);
        }

        return new ActualFilesInterval(left, right);
    }

    static void changeActualLeftInterval(
            final Path indexFile,
            final Arena arena,
            final long left
    ) throws IOException {
        try (FileChannel fileChannel = FileChannel.open(indexFile,
                StandardOpenOption.READ,
                StandardOpenOption.WRITE,
                StandardOpenOption.CREATE,
                StandardOpenOption.TRUNCATE_EXISTING
        )) {
            MemorySegment fileSegment = fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, 2 * Long.BYTES, arena);
            fileSegment.setAtIndex(ValueLayout.JAVA_LONG_UNALIGNED, 0, left);
            if (left >= fileSegment.getAtIndex(ValueLayout.JAVA_LONG_UNALIGNED, 1)) {
                fileSegment.setAtIndex(ValueLayout.JAVA_LONG_UNALIGNED, 1, left + 1);
            }
        }
    }

    static void changeActualFilesInterval(
            final Path indexFile,
            final Arena arena,
            final long left,
            final long right
    ) throws IOException {
        try (FileChannel fileChannel = FileChannel.open(indexFile,
                StandardOpenOption.READ,
                StandardOpenOption.WRITE,
                StandardOpenOption.CREATE,
                StandardOpenOption.TRUNCATE_EXISTING
        )) {
            MemorySegment fileSegment = fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, 2 * Long.BYTES, arena);
            fileSegment.setAtIndex(ValueLayout.JAVA_LONG_UNALIGNED, 0, left);
            fileSegment.setAtIndex(ValueLayout.JAVA_LONG_UNALIGNED, 1, right);
        }
    }

    static void deleteFilesAndInMemory(
            List<MemorySegment> segmentList,
            ActualFilesInterval interval,
            Path storagePath
    ) throws IOException {
        for (long i = interval.left(); i < interval.right(); ++i) {
            Files.delete(storagePath.resolve(Long.toString(i)));
        }
        segmentList.clear();
    }

    // gets first element >= after key
    public static long indexOf(MemorySegment segment, MemorySegment key) {
        long recordsCount = recordsCount(segment);

        long left = 0;
        long right = recordsCount - 1;
        while (left <= right) {
            long mid = (left + right) >>> 1;

            long startOfKey = startOfKey(segment, mid);
            long endOfKey = endOfKey(segment, mid);
            long mismatch = MemorySegment.mismatch(segment, startOfKey, endOfKey, key, 0, key.byteSize());
            if (mismatch == -1) {
                return mid;
            }

            if (mismatch == key.byteSize()) {
                right = mid - 1;
                continue;
            }

            if (mismatch == endOfKey - startOfKey) {
                left = mid + 1;
                continue;
            }

            int b1 = Byte.toUnsignedInt(segment.get(ValueLayout.JAVA_BYTE, startOfKey + mismatch));
            int b2 = Byte.toUnsignedInt(key.get(ValueLayout.JAVA_BYTE, mismatch));
            if (b1 <= b2) {
                left = mid + 1;
            } else {
                right = mid - 1;
            }
        }

        return DiskStorage.tombstone(left);
    }

    // getting count of records
    static long recordsCount(MemorySegment segment) {
        long indexSize = indexSize(segment);
        return indexSize / Long.BYTES / 2;
    }

    // getting first offset of key
    // TODO do normal
    private static long indexSize(MemorySegment segment) {
        try {
            return segment.get(ValueLayout.JAVA_LONG_UNALIGNED, 0);
        } catch (IndexOutOfBoundsException e) {
            return 0L;
        }
    }

    static MemorySegment slice(MemorySegment page, long start, long end) {
        return page.asSlice(start, end - start);
    }

    static long startOfKey(MemorySegment segment, long recordIndex) {
        return segment.getAtIndex(ValueLayout.JAVA_LONG_UNALIGNED, recordIndex * 2);
    }

    static long startOfValue(MemorySegment segment, long recordIndex) {
        return segment.getAtIndex(ValueLayout.JAVA_LONG_UNALIGNED, recordIndex * 2 + 1);
    }

    static long endOfKey(MemorySegment segment, long recordIndex) {
        return normalizedStartOfValue(segment, recordIndex);
    }

    private static long normalizedStartOfValue(MemorySegment segment, long recordIndex) {
        return normalize(startOfValue(segment, recordIndex));
    }

    static long endOfValue(MemorySegment segment, long recordIndex, long recordsCount) {
        if (recordIndex < recordsCount - 1) {
            return startOfKey(segment, recordIndex + 1);
        }
        return segment.byteSize();
    }

    // we do this for point that it's the tombstone
    static long tombstone(long offset) {
        return 1L << 63 | offset;
    }

    static long normalize(long value) {
        return value & ~(1L << 63);
    }
}
