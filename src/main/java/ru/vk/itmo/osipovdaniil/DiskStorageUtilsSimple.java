package ru.vk.itmo.osipovdaniil;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.file.Path;

public final class DiskStorageUtilsSimple {

    private static final String INDEX = "index.idx";
    private static final String INDEX_TMP = "index.tmp";

    private DiskStorageUtilsSimple() {
    }

    public static Path getIndexTmpPath(final Path storagePath) {
        return storagePath.resolve(INDEX_TMP);
    }

    public static Path getIndexPath(final Path storagePath) {
        return storagePath.resolve(INDEX);
    }

    static long recordsCount(final MemorySegment segment) {
        long indexSize = indexSize(segment);
        return indexSize / Long.BYTES / 2;
    }

    static long indexSize(final MemorySegment segment) {
        return segment.get(ValueLayout.JAVA_LONG_UNALIGNED, 0);
    }

    static MemorySegment slice(final MemorySegment page, final long start, final long end) {
        return page.asSlice(start, end - start);
    }

    static long startOfKey(final MemorySegment segment, final long recordIndex) {
        return segment.get(ValueLayout.JAVA_LONG_UNALIGNED, recordIndex * 2 * Long.BYTES);
    }

    static long endOfKey(final MemorySegment segment, final long recordIndex) {
        return normalizedStartOfValue(segment, recordIndex);
    }

    static long normalizedStartOfValue(final MemorySegment segment, final long recordIndex) {
        return normalize(startOfValue(segment, recordIndex));
    }

    static long startOfValue(final MemorySegment segment, final long recordIndex) {
        return segment.get(ValueLayout.JAVA_LONG_UNALIGNED, recordIndex * 2 * Long.BYTES + Long.BYTES);
    }

    static long endOfValue(final MemorySegment segment, final long recordIndex, final long recordsCount) {
        if (recordIndex < recordsCount - 1) {
            return startOfKey(segment, recordIndex + 1);
        }
        return segment.byteSize();
    }

    static long tombstone(final long offset) {
        return 1L << 63 | offset;
    }

    static long normalize(final long value) {
        return value & ~(1L << 63);
    }
}
