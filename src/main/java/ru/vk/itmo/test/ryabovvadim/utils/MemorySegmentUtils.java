package ru.vk.itmo.test.ryabovvadim.utils;

import java.lang.foreign.MemorySegment;

import static java.lang.foreign.ValueLayout.JAVA_BYTE;

public final class MemorySegmentUtils {

    public static int compareMemorySegments(MemorySegment l, MemorySegment r) {
        return compareMemorySegments(
                l, 0, l.byteSize(),
                r, 0, r.byteSize()
        );
    }

    public static int compareMemorySegments(
            MemorySegment l,
            long lFromOffset,
            long lToOffset,
            MemorySegment r,
            long rFromOffset,
            long rToOffset
    ) {
        if (l == null) {
            return r == null ? 0 : -1;
        } else if (r == null) {
            return 1;
        }

        long lSize = lToOffset - lFromOffset;
        long rSize = rToOffset - rFromOffset;
        long mismatch = MemorySegment.mismatch(
                l, lFromOffset, lToOffset,
                r, rFromOffset, rToOffset
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
                l.get(JAVA_BYTE, lFromOffset + mismatch),
                r.get(JAVA_BYTE, rFromOffset + mismatch)
        );
    }


    private MemorySegmentUtils() {
    }
}
