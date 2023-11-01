package ru.vk.itmo.test.ryabovvadim.utils;

import java.lang.foreign.MemorySegment;

import static java.lang.foreign.ValueLayout.JAVA_BYTE;

public final class MemorySegmentUtils {

    public static int compareMemorySegments(MemorySegment left, MemorySegment right) {
        return compareMemorySegments(
                left, 0, left.byteSize(),
                right, 0, right.byteSize()
        );
    }

    public static int compareMemorySegments(
            MemorySegment left,
            long leftFromOffset,
            long leftToOffset,
            MemorySegment right,
            long rightFromOffset,
            long rightToOffset
    ) {
        if (left == null) {
            return right == null ? 0 : -1;
        } else if (right == null) {
            return 1;
        }

        long lSize = leftToOffset - leftFromOffset;
        long rSize = rightToOffset - rightFromOffset;
        long mismatch = MemorySegment.mismatch(
                left, leftFromOffset, leftToOffset,
                right, rightFromOffset, rightToOffset
        );

        if (mismatch == lSize) {
            return -1;
        }
        if (mismatch == rSize) {
            return 1;
        }
        if (mismatch == -1) {
            return 0;
        }

        return Byte.compareUnsigned(
                left.get(JAVA_BYTE, leftFromOffset + mismatch),
                right.get(JAVA_BYTE, rightFromOffset + mismatch)
        );
    }

    private MemorySegmentUtils() {
    }
}
