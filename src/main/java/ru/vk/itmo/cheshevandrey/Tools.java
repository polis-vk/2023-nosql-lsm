package ru.vk.itmo.cheshevandrey;

import ru.vk.itmo.Entry;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

public final class Tools {

    private static final String DELIMITER = "_";
    private static final String EXTENSION = ".sst";

    // To hide the implicit public constructor.
    private Tools() {
    }

    static Entry<MemorySegment> entryToReturn(Entry<MemorySegment> entry) {
        if (entry.value() == null) {
            return null;
        }
        return entry;
    }

    static int compare(MemorySegment memorySegment1, MemorySegment memorySegment2) {
        long mismatch = memorySegment1.mismatch(memorySegment2);
        if (mismatch == -1) {
            return 0;
        }

        if (mismatch == memorySegment1.byteSize()) {
            return -1;
        }

        if (mismatch == memorySegment2.byteSize()) {
            return 1;
        }
        byte b1 = memorySegment1.get(ValueLayout.JAVA_BYTE, mismatch);
        byte b2 = memorySegment2.get(ValueLayout.JAVA_BYTE, mismatch);
        return Byte.compare(b1, b2);
    }

    static long recordsCount(MemorySegment segment) {
        long indexSize = indexSize(segment);
        return indexSize / Long.BYTES / 2;
    }

    static long indexSize(MemorySegment segment) {
        return segment.get(ValueLayout.JAVA_LONG_UNALIGNED, 0);
    }

    static String getFileName(int ssTableNumber, int compactNumber) {
        return ssTableNumber + DELIMITER + compactNumber + EXTENSION;
    }

    static MemorySegment slice(MemorySegment page, long start, long end) {
        return page.asSlice(start, end - start);
    }

    static long startOfKey(MemorySegment segment, long recordIndex) {
        return segment.get(ValueLayout.JAVA_LONG_UNALIGNED, recordIndex * 2 * Long.BYTES);
    }

    static long endOfKey(MemorySegment segment, long recordIndex) {
        return normalizedStartOfValue(segment, recordIndex);
    }

    static long normalizedStartOfValue(MemorySegment segment, long recordIndex) {
        return normalize(startOfValue(segment, recordIndex));
    }

    static long startOfValue(MemorySegment segment, long recordIndex) {
        return segment.get(ValueLayout.JAVA_LONG_UNALIGNED, recordIndex * 2 * Long.BYTES + Long.BYTES);
    }

    static long endOfValue(MemorySegment segment, long recordIndex, long recordsCount) {
        if (recordIndex < recordsCount - 1) {
            return startOfKey(segment, recordIndex + 1);
        }
        return segment.byteSize();
    }

    static long tombstone(long offset) {
        return 1L << 63 | offset;
    }

    static long normalize(long value) {
        return value & ~(1L << 63);
    }

}
