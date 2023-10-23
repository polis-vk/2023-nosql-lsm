package ru.vk.itmo.prokopyevnikita;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

public final class MemorySegmentComparator {
    private MemorySegmentComparator() {
    }

    public static int compare(MemorySegment o1, MemorySegment o2) {
        return compareWithOffsets(o1, 0, o1.byteSize(), o2, 0, o2.byteSize());
    }

    public static int compareWithOffsets(
            MemorySegment o1, long o1FromOffset, long o1ToOffset,
            MemorySegment o2, long o2FromOffset, long o2ToOffset) {
        long relativeOffset = MemorySegment.mismatch(o1, o1FromOffset, o1ToOffset, o2, o2FromOffset, o2ToOffset);
        if (relativeOffset == -1) {
            return 0;
        } else if (relativeOffset == o1.byteSize()) {
            return -1;
        } else if (relativeOffset == o2.byteSize()) {
            return 1;
        }
        return Byte.compare(
                o1.get(ValueLayout.JAVA_BYTE, o1FromOffset + relativeOffset),
                o2.get(ValueLayout.JAVA_BYTE, o2FromOffset + relativeOffset)
        );
    }
}
