package ru.vk.itmo.dalbeevgeorgii;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

public final class MemorySegmentComparator {
    private MemorySegmentComparator() {
    }

    public static int compare(MemorySegment memorySegment1, MemorySegment memorySegment2) {
        return compareWithOffsets(
                memorySegment1,
                0,
                memorySegment1.byteSize(),
                memorySegment2
        );
    }

    public static int compareWithOffsets(
            MemorySegment memorySegment1,
            long memorySegment1FromOffset,
            long memorySegment1ToOffset,
            MemorySegment memorySegment2
    ) {
        long relativeOffset = MemorySegment.mismatch(
                memorySegment1,
                memorySegment1FromOffset,
                memorySegment1ToOffset,
                memorySegment2,
                0,
                memorySegment2.byteSize()
        );
        if (relativeOffset == -1) {
            return 0;
        } else if (relativeOffset == memorySegment1.byteSize()) {
            return -1;
        } else if (relativeOffset == memorySegment2.byteSize()) {
            return 1;
        }
        return Byte.compare(
                memorySegment1.get(ValueLayout.JAVA_BYTE, memorySegment1FromOffset + relativeOffset),
                memorySegment2.get(ValueLayout.JAVA_BYTE, relativeOffset)
        );
    }
}
