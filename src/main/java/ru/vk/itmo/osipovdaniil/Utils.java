package ru.vk.itmo.osipovdaniil;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

public final class Utils {

    private Utils() {
    }

    public static int compareMemorySegments(final MemorySegment a, final MemorySegment b) {
        long mismatchOffset = a.mismatch(b);
        if (mismatchOffset == -1) {
            return 0;
        } else if (mismatchOffset == a.byteSize()) {
            return -1;
        } else if (mismatchOffset == b.byteSize()) {
            return 1;
        } else {
            return Byte.compare(a.getAtIndex(ValueLayout.JAVA_BYTE, mismatchOffset),
                    b.getAtIndex(ValueLayout.JAVA_BYTE, mismatchOffset));
        }
    }
}
