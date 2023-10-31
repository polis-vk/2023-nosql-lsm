package ru.vk.itmo.supriadkinadaria;

import ru.vk.itmo.BaseEntry;
import ru.vk.itmo.Entry;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

public final class DiskStorageUtils {
    private DiskStorageUtils() {

    }

    public static MemorySegment slice(MemorySegment page, long start, long end) {
        return page.asSlice(start, end - start);
    }

    public static long startOfKey(MemorySegment segment, long recordIndex) {
        return segment.get(ValueLayout.JAVA_LONG_UNALIGNED, recordIndex * 2 * Long.BYTES);
    }

    public static long endOfKey(MemorySegment segment, long recordIndex) {
        return normalizedStartOfValue(segment, recordIndex);
    }

    private static long normalizedStartOfValue(MemorySegment segment, long recordIndex) {
        return normalize(startOfValue(segment, recordIndex));
    }

    public static long startOfValue(MemorySegment segment, long recordIndex) {
        return segment.get(ValueLayout.JAVA_LONG_UNALIGNED, recordIndex * 2 * Long.BYTES + Long.BYTES);
    }

    public static long endOfValue(MemorySegment segment, long recordIndex, long recordsCount) {
        if (recordIndex < recordsCount - 1) {
            return startOfKey(segment, recordIndex + 1);
        }
        return segment.byteSize();
    }

    public static long tombstone(long offset) {
        return 1L << 63 | offset;
    }

    public static long normalize(long value) {
        return value & ~(1L << 63);
    }

    private static long indexOf(MemorySegment segment, MemorySegment key) {
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
            if (b1 > b2) {
                right = mid - 1;
            } else {
                left = mid + 1;
            }
        }

        return tombstone(left);
    }

    public static long recordsCount(MemorySegment segment) {
        long indexSize = indexSize(segment);
        return indexSize / Long.BYTES / 2;
    }

    private static long indexSize(MemorySegment segment) {
        return segment.byteSize() > 0 ? segment.get(ValueLayout.JAVA_LONG_UNALIGNED, 0) : 0;
    }

    private static Iterator<Entry<MemorySegment>> iterator(MemorySegment page, MemorySegment from, MemorySegment to) {
        long recordIndexFrom = from == null ? 0 : normalize(indexOf(page, from));
        long recordIndexTo = to == null ? recordsCount(page) : normalize(indexOf(page, to));
        long recordsCount = recordsCount(page);

        return new Iterator<>() {
            long index = recordIndexFrom;

            @Override
            public boolean hasNext() {
                return index < recordIndexTo;
            }

            @Override
            public Entry<MemorySegment> next() {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }
                MemorySegment key = slice(page, startOfKey(page, index), endOfKey(page, index));
                long startOfValue = startOfValue(page, index);
                MemorySegment value =
                        startOfValue < 0
                                ? null
                                : slice(page, startOfValue, endOfValue(page, index, recordsCount));
                index++;
                return new BaseEntry<>(key, value);
            }
        };
    }

    public static Iterator<Entry<MemorySegment>> range(
            Iterator<Entry<MemorySegment>> firstIterator,
            MemorySegment from,
            MemorySegment to,
            List<MemorySegment> segmentList) {
        List<Iterator<Entry<MemorySegment>>> iterators = new ArrayList<>(segmentList.size() + 1);
        for (MemorySegment memorySegment : segmentList) {
            iterators.add(iterator(memorySegment, from, to));
        }
        iterators.add(firstIterator);
        return new MergeIterator(iterators, Comparator.comparing(Entry::key, MemorySegmentDao::compare)) {
            @Override
            protected boolean skip(Entry<MemorySegment> memorySegmentEntry) {
                return memorySegmentEntry.value() == null;
            }
        };
    }
}
