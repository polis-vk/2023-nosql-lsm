package ru.vk.itmo.smirnovdmitrii.outofmemory.sstable;

import ru.vk.itmo.BaseEntry;
import ru.vk.itmo.Entry;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.Comparator;

/**
 * Utils works only with opened SSTable.
 */
public final class SSTableUtil {

    private SSTableUtil() {
    }

    public static Entry<MemorySegment> readBlock(final SSTable ssTable, final long index) {
        return new BaseEntry<>(
                readBlockKey(ssTable, index),
                readBlockValue(ssTable, index)
        );
    }

    public static MemorySegment readBlockKey(final SSTable ssTable, final long index) {
        final long startOfKey = startOfKey(ssTable, index);
        return ssTable.mapped().asSlice(startOfKey, endOfKey(ssTable, index) - startOfKey);
    }

    private static MemorySegment readBlockValue(final SSTable ssTable, final long index) {
        final long startOfValue = startOfValue(ssTable, index);
        if (startOfValue < 0) {
            return null;
        }
        return ssTable.mapped().asSlice(startOfValue, endOfValue(ssTable, index) - startOfValue);
    }

    private static long startOfKey(final SSTable ssTable, final long index) {
        return ssTable.mapped().get(ValueLayout.JAVA_LONG_UNALIGNED, index * Long.BYTES * 2);
    }

    private static long startOfValue(final SSTable ssTable, final long index) {
        return ssTable.mapped().get(ValueLayout.JAVA_LONG_UNALIGNED, index * Long.BYTES * 2 + Long.BYTES);
    }

    private static long normalizedStartOfValue(final SSTable ssTable, final long index) {
        return normalize(startOfValue(ssTable, index));
    }

    private static long endOfKey(final SSTable ssTable, final long index) {
        return normalizedStartOfValue(ssTable, index);
    }

    private static long endOfValue(final SSTable ssTable, final long index) {
        if (index == blockCount(ssTable) - 1) {
            return ssTable.mapped().byteSize();
        }
        return startOfKey(ssTable, index + 1);
    }

    public static long blockCount(final SSTable ssTable) {
        return ssTable.mapped().get(ValueLayout.JAVA_LONG_UNALIGNED, 0) / Long.BYTES / 2;
    }

    public static long tombstone(final long value) {
        return value | 1L << 63;
    }

    public static long normalize(final long value) {
        return value & ~(1L << 63);
    }

    /**
     * Searching order number in ssTable for block with {@code key} using helping file with ssTable offsets.
     * If there is no block with such key, returns -(insert position + 1).
     * {@code offsets}.
     * @param key searching key.
     * @param ssTable sstable.
     * @return offset in sstable for key block.
     */
    public static long binarySearch(
            final MemorySegment key,
            final SSTable ssTable,
            final Comparator<MemorySegment> comparator
    ) {
        long left = -1;
        long right = blockCount(ssTable);
        while (left < right - 1) {
            long midst = (left + right) >>> 1;
            final MemorySegment currentKey = readBlockKey(ssTable, midst);
            final int compareResult = comparator.compare(key, currentKey);
            if (compareResult == 0) {
                return midst;
            } else if (compareResult > 0) {
                left = midst;
            } else {
                right = midst;
            }
        }
        return tombstone(right);
    }

    public static long upperBound(
            final MemorySegment key,
            final SSTable ssTable,
            final Comparator<MemorySegment> comparator
    ) {
        final long result = binarySearch(key, ssTable, comparator);
        return normalize(result);
    }
}
