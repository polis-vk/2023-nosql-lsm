package ru.vk.itmo.podobaalexandr;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

public final class MemorySegmentUtils {

    private MemorySegmentUtils() {

    }

    public static int compare(MemorySegment o1, MemorySegment o2) {
        return compareSegments(o1, o1.byteSize(), o2, 0, o2.byteSize());
    }

    public static int compareSegments(MemorySegment src, long srcEnd,
                                      MemorySegment dest, long destOffset, long destEnd) {
        long destLength = destEnd - destOffset;

        int sizeDiff = Long.compare(srcEnd, destLength);

        if (srcEnd == 0 || destLength == 0) {
            return sizeDiff;
        }

        long mismatch = MemorySegment.mismatch(src, 0, srcEnd, dest, destOffset, destEnd);

        if (mismatch == destLength || mismatch == srcEnd) {
            return sizeDiff;
        }

        return mismatch == -1
                ? 0
                : Byte.compare(src.get(ValueLayout.JAVA_BYTE, mismatch),
                dest.get(ValueLayout.JAVA_BYTE, destOffset + mismatch));
    }

}
