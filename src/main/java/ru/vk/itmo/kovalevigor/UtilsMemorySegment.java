package ru.vk.itmo.kovalevigor;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

public final class UtilsMemorySegment {

    private UtilsMemorySegment() {
    }

    private static byte getByte(final MemorySegment memorySegment, final long offset) {
        return memorySegment.get(ValueLayout.JAVA_BYTE, offset);
    }

    public static long findDiff(final MemorySegment lhs, final MemorySegment rhs) {
        return lhs.mismatch(rhs);
    }

    public static int compare(final MemorySegment lhs, final MemorySegment rhs) {
        final long mismatch = findDiff(lhs, rhs);
        final long lhsSize = lhs.byteSize();
        final long rhsSize = rhs.byteSize();
        final long minSize = Math.min(lhsSize, rhsSize);
        if (mismatch == -1) {
            return 0;
        } else if (minSize == mismatch) {
            return Long.compare(lhsSize, rhsSize);
        }
        return Byte.compare(getByte(lhs, mismatch), getByte(rhs, mismatch));
    }
}
