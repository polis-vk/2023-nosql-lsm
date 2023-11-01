package ru.vk.itmo.volkovnikita;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.Comparator;

import static java.lang.foreign.ValueLayout.JAVA_BYTE;

public final class MemorySegmentComparator {
    private MemorySegmentComparator() {
    }

    public static int compare(MemorySegment memorySegment1, MemorySegment memorySegment2) {
        return compareOffsets(
                0L,
                memorySegment1.byteSize(),
                memorySegment1,
                memorySegment2
        );
    }

    public static int compareOffsets(
                              long fromOffset1,
                              long toOffSet1,
                              MemorySegment memorySegment1,
                              MemorySegment memorySegment2) {
        long fromOffset2 = 0L;
        long mismatchOffSet = MemorySegment.mismatch(
                memorySegment1,
                fromOffset1,
                toOffSet1,
                memorySegment2,
                fromOffset2,
                memorySegment2.byteSize()
        );

        if (mismatchOffSet == -1) {
            return 0;
        } else if (mismatchOffSet == memorySegment1.byteSize()) {
            return -1;
        } else if (mismatchOffSet == memorySegment2.byteSize()) {
            return 1;
        }
        return Byte.compare(
                memorySegment1.get(ValueLayout.JAVA_BYTE, fromOffset1 + mismatchOffSet),
                memorySegment2.get(ValueLayout.JAVA_BYTE, mismatchOffSet)
        );
    }
}
