package ru.vk.itmo.bazhenovkirill;

import ru.vk.itmo.BaseEntry;
import ru.vk.itmo.Entry;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

public final class MemorySegmentUtils {

    private MemorySegmentUtils() {
        
    }

    public static MemorySegment getSlice(MemorySegment segment, long start, long end) {
        return segment.asSlice(start, end - start);
    }

    public static long startOfKey(MemorySegment segment, long inx) {
        return segment.get(ValueLayout.JAVA_LONG_UNALIGNED, inx * 2 * Long.BYTES);
    }

    public static long endOfKey(MemorySegment segment, long inx) {
        return normalize(startOfValue(segment, inx));
    }

    public static MemorySegment getKey(MemorySegment segment, long inx) {
        return getSlice(segment, startOfKey(segment, inx), endOfKey(segment, inx));
    }

    public static long startOfValue(MemorySegment segment, long inx) {
        return segment.get(ValueLayout.JAVA_LONG_UNALIGNED, Long.BYTES + inx * 2 * Long.BYTES);
    }

    public static long endOfValue(MemorySegment segment, long inx) {
        if (inx < recordsCount(segment) - 1) {
            return startOfKey(segment, inx + 1);
        }
        return segment.byteSize();
    }

    public static Entry<MemorySegment> getEntry(MemorySegment segment, long inx) {
        MemorySegment key = getKey(segment, inx);
        MemorySegment value = getValue(segment, inx);
        return new BaseEntry<>(key, value);
    }

    public static MemorySegment getValue(MemorySegment segment, long inx) {
        long start = startOfValue(segment, inx);
        if (start < 0) {
            return null;
        }
        return getSlice(segment, start, endOfValue(segment, inx));
    }

    public static long tombstone(long offset) {
        return 1L << 63 | offset;
    }

    public static long normalize(long offset) {
        return offset & ~(1L << 63);
    }

    public static long recordsCount(MemorySegment segment) {
        return indexSize(segment) / (2 * Long.BYTES);
    }

    public static long indexSize(MemorySegment segment) {
        return segment.get(ValueLayout.JAVA_LONG_UNALIGNED, 0);
    }

}
