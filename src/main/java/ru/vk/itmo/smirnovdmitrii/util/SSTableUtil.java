package ru.vk.itmo.smirnovdmitrii.util;

import ru.vk.itmo.BaseEntry;
import ru.vk.itmo.Entry;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

public final class SSTableUtil {

    private SSTableUtil() {
    }

    public static Entry<MemorySegment> readBlock(final MemorySegment sstable, final long index) {
        return new BaseEntry<>(
                readBlockKey(sstable, index),
                readBlockValue(sstable, index)
        );
    }

    public static MemorySegment readBlockKey(final MemorySegment sstable, final long index) {
        final long startOfKey = startOfKey(sstable, index);
        return sstable.asSlice(startOfKey, endOfKey(sstable, index) - startOfKey);
    }

    private static MemorySegment readBlockValue(final MemorySegment sstable, final long index) {
        final long startOfValue = startOfValue(sstable, index);
        if (startOfValue < 0) {
            return null;
        }
        return sstable.asSlice(startOfValue, endOfValue(sstable, index) - startOfValue);
    }

    private static long startOfKey(final MemorySegment sstable, final long index) {
        return sstable.get(ValueLayout.JAVA_LONG_UNALIGNED, index * Long.BYTES * 2);
    }

    private static long startOfValue(final MemorySegment sstable, final long index) {
        return sstable.get(ValueLayout.JAVA_LONG_UNALIGNED, index * Long.BYTES * 2 + Long.BYTES);
    }

    private static long normalizedStartOfValue(final MemorySegment sstable, final long index) {
        return normalize(startOfValue(sstable, index));
    }

    private static long endOfKey(final MemorySegment sstable, final long index) {
        return normalizedStartOfValue(sstable, index);
    }

    private static long endOfValue(final MemorySegment sstable, final long index) {
        if (index == blockCount(sstable) - 1) {
            return sstable.byteSize();
        }
        return startOfKey(sstable, index + 1);
    }

    public static long blockCount(final MemorySegment sstable) {
        return sstable.get(ValueLayout.JAVA_LONG_UNALIGNED, 0) / Long.BYTES / 2;
    }

    public static long tombstone(final long value) {
        return value | 1L << 63;
    }

    public static long normalize(final long value) {
        return value & ~(1L << 63);
    }
}
