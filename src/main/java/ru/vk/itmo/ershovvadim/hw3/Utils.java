package ru.vk.itmo.ershovvadim.hw3;

import java.lang.foreign.MemorySegment;

import static java.lang.foreign.ValueLayout.JAVA_BYTE;

public final class Utils {

    private Utils() {
        throw new IllegalStateException("Utility class");
    }

    public static int compare(MemorySegment segment1, MemorySegment segment2) {
        return compare(segment1, segment2, 0, segment2.byteSize());
    }

    public static int compare(MemorySegment segment1, MemorySegment segment2, long fromOffset, long toOffset) {
        long mismatchOffset =
                MemorySegment.mismatch(
                        segment1, 0, segment1.byteSize(),
                        segment2, fromOffset, toOffset
                );
        if (mismatchOffset == -1) {
            return 0;
        } else if (mismatchOffset == segment1.byteSize()) {
            return -1;
        } else if (mismatchOffset == segment2.byteSize()) {
            return 1;
        }

        var offsetByte1 = segment1.get(JAVA_BYTE, mismatchOffset);
        var offsetByte2 = segment2.get(JAVA_BYTE, fromOffset + mismatchOffset);
        return Byte.compare(offsetByte1, offsetByte2);
    }
}
