package ru.vk.itmo.chebotinalexandr;

import ru.vk.itmo.BaseEntry;
import ru.vk.itmo.Entry;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;


public final class SSTableUtils {
    public static final long TOMBSTONE = -1;
    public static final long OLDEST_SS_TABLE_INDEX = 0;
    public static final int SS_TABLE_PRIORITY = 1;
    public static final long COMPACTION_NOT_FINISHED_TAG = -1;
    public static final long BLOOM_FILTER_LENGTH_OFFSET = 0;
    public static final long BLOOM_FILTER_BIT_SIZE_OFFSET = Long.BYTES;
    public static final long BLOOM_FILTER_HASH_FUNCTIONS_OFFSET = 2L * Long.BYTES;
    public static final long ENTRIES_SIZE_OFFSET = 3L * Long.BYTES;

    private SSTableUtils() {

    }

    public static long binarySearch(MemorySegment readSegment, MemorySegment key) {
        long low = -1;
        long high = readSegment.get(ValueLayout.JAVA_LONG_UNALIGNED, ENTRIES_SIZE_OFFSET);

        final long bloomFilterLength = readSegment.get(ValueLayout.JAVA_LONG_UNALIGNED, BLOOM_FILTER_LENGTH_OFFSET);
        final long keyOffset = 4L * Long.BYTES + bloomFilterLength * Long.BYTES;

        while (low < high - 1) {
            long mid = (high - low) / 2 + low;

            long offset = readSegment.get(ValueLayout.JAVA_LONG_UNALIGNED, keyOffset + mid * Long.BYTES);
            long keySize = readSegment.get(ValueLayout.JAVA_LONG_UNALIGNED, offset);
            offset += Long.BYTES;

            long mismatch = MemorySegment.mismatch(readSegment, offset, offset + keySize,
                    key, 0, key.byteSize());

            if (mismatch == -1) {
                return mid;
            }

            if (mismatch == keySize) {
                low = mid;
                continue;
            }
            if (mismatch == key.byteSize()) {
                high = mid;
                continue;
            }

            int compare = Byte.compare(readSegment.get(ValueLayout.JAVA_BYTE, offset + mismatch),
                    key.get(ValueLayout.JAVA_BYTE, mismatch));

            if (compare > 0) {
                high = mid;
            } else {
                low = mid;
            }
        }

        return low + 1;
    }

    public static Entry<MemorySegment> get(MemorySegment readSegment, MemorySegment key) {
        long low = -1;
        long high = readSegment.get(ValueLayout.JAVA_LONG_UNALIGNED, ENTRIES_SIZE_OFFSET);

        final long bloomFilterLength = readSegment.get(ValueLayout.JAVA_LONG_UNALIGNED, BLOOM_FILTER_LENGTH_OFFSET);
        final long keyOffset = 4L * Long.BYTES + Long.BYTES * bloomFilterLength;

        while (low < high - 1) {
            long mid = (high - low) / 2 + low;

            long offset = readSegment.get(ValueLayout.JAVA_LONG_UNALIGNED, keyOffset + mid * Long.BYTES);
            long keySize = readSegment.get(ValueLayout.JAVA_LONG_UNALIGNED, offset);
            offset += Long.BYTES;

            long mismatch = MemorySegment.mismatch(readSegment, offset, offset + keySize,
                    key, 0, key.byteSize());

            if (mismatch == -1) {
                return get(readSegment, mid, keyOffset);
            }

            if (mismatch == keySize) {
                low = mid;
                continue;
            }
            if (mismatch == key.byteSize()) {
                high = mid;
                continue;
            }

            int compare = Byte.compare(readSegment.get(ValueLayout.JAVA_BYTE, offset + mismatch),
                    key.get(ValueLayout.JAVA_BYTE, mismatch));

            if (compare > 0) {
                high = mid;
            } else {
                low = mid;
            }
        }

        return null;
    }

    private static Entry<MemorySegment> get(MemorySegment sstable, long index, final long afterBloomFilterOffset) {
        long offset = afterBloomFilterOffset + index * Byte.SIZE;

        long keyOffset = sstable.get(ValueLayout.JAVA_LONG_UNALIGNED, offset);
        long keySize = sstable.get(ValueLayout.JAVA_LONG_UNALIGNED, keyOffset);
        keyOffset += Long.BYTES;
        MemorySegment key = sstable.asSlice(keyOffset, keySize);
        keyOffset += keySize;
        long valueSize = sstable.get(ValueLayout.JAVA_LONG_UNALIGNED, keyOffset);
        keyOffset += Long.BYTES;

        if (valueSize == TOMBSTONE) {
            return new BaseEntry<>(key, null);
        } else {
            return new BaseEntry<>(key, sstable.asSlice(keyOffset, valueSize));
        }
    }

    public static long entryByteSize(Entry<MemorySegment> entry) {
        if (entry.value() == null) {
            return entry.key().byteSize();
        }

        return entry.key().byteSize() + entry.value().byteSize();
    }
}
