package ru.vk.itmo.bandurinvladislav;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

public final class StorageUtil {

    public static long indexOf(MemorySegment segment, MemorySegment key) {
        long bloomSize = bloomSize(segment);
        long recordsCount = recordsCount(segment, bloomSize);

        long left = 0;
        long right = recordsCount - 1;
        while (left <= right) {
            long mid = (left + right) >>> 1;

            long startOfKey = startOfKey(segment, mid, bloomSize);
            long endOfKey = endOfKey(segment, mid, bloomSize);
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
            if (b1 > b2) {
                right = mid - 1;
            } else {
                left = mid + 1;
            }
        }

        return tombstone(left);
    }

    public static long recordsCount(MemorySegment segment, long bloomSize) {
        long indexSize = indexSize(segment, bloomSize);
        return indexSize / Long.BYTES / 2;
    }

    public static long bloomSize(MemorySegment segment) {
        return segment.get(ValueLayout.JAVA_LONG_UNALIGNED, 0);
    }

    public static long indexSize(MemorySegment segment, long bloomSize) {
        return segment.get(ValueLayout.JAVA_LONG_UNALIGNED, bloomSize) - bloomSize;
    }

    public static MemorySegment slice(MemorySegment page, long start, long end) {
        return page.asSlice(start, end - start);
    }

    public static long startOfKey(MemorySegment segment, long recordIndex, long bloomSize) {
        return segment.get(ValueLayout.JAVA_LONG_UNALIGNED, bloomSize + recordIndex * 2 * Long.BYTES);
    }

    public static long endOfKey(MemorySegment segment, long recordIndex, long bloomSize) {
        return normalizedStartOfValue(segment, recordIndex, bloomSize);
    }

    public static long normalizedStartOfValue(MemorySegment segment, long recordIndex, long bloomSize) {
        return normalize(startOfValue(segment, recordIndex, bloomSize));
    }

    public static long startOfValue(MemorySegment segment, long recordIndex, long bloomSize) {
        return segment.get(ValueLayout.JAVA_LONG_UNALIGNED, bloomSize + recordIndex * 2 * Long.BYTES + Long.BYTES);
    }

    public static long endOfValue(MemorySegment segment, long recordIndex, long recordsCount, long bloomSize) {
        if (recordIndex < recordsCount - 1) {
            return startOfKey(segment, recordIndex + 1, bloomSize);
        }
        return segment.byteSize();
    }

    public static long tombstone(long offset) {
        return 1L << 63 | offset;
    }

    public static long normalize(long value) {
        return value & ~(1L << 63);
    }

    public static boolean checkBloom(MemorySegment page, MemorySegment key) {
        long bloomSize = bloomSize(page);
        int bitsetSize = (int) (bloomSize - Long.BYTES) * 8;
        int hashCount = BloomUtil.evalHashCount(bitsetSize, (int) recordsCount(page, bloomSize));
        long[] indexes = new long[hashCount];
        BloomUtil.hashKey(key, indexes);
        BloomFilter.indexes(indexes[1], indexes[0], hashCount, bitsetSize, indexes);

        return checkIndexes(page, indexes);
    }

    public static boolean checkIndexes(MemorySegment page, long[] indexes) {
        for (long index : indexes) {
            int pageOffset = (int) index >> 6;
            if (!checkBit(
                    page.get(ValueLayout.JAVA_LONG_UNALIGNED, Long.BYTES + pageOffset * Long.BYTES),
                    index - (pageOffset << 6)
            )) {
                return false;
            }
        }
        return true;
    }

    public static boolean checkBit(long value, long i) {
        return (value & (1L << (63 - i))) != 0;
    }
    
    private StorageUtil() {
    }
}
