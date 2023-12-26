package ru.vk.itmo.tuzikovalexandr;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

public final class MemorySegmentComparator {

    private MemorySegmentComparator() {
    }

    public static int compare(MemorySegment o1, MemorySegment o2) {
        long offset = o1.mismatch(o2);
        if (offset == -1) {
            return 0;
        }
        if (offset == o2.byteSize()) {
            return 1;
        }
        if (offset == o1.byteSize()) {
            return -1;
        }

        return Byte.compare(o1.get(ValueLayout.JAVA_BYTE, offset), o2.get(ValueLayout.JAVA_BYTE, offset));
    }
}
