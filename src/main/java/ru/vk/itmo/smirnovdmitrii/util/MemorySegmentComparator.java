package ru.vk.itmo.smirnovdmitrii.util;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.Comparator;

public final class MemorySegmentComparator implements Comparator<MemorySegment> {

    @Override
    public int compare(final MemorySegment o1, final MemorySegment o2) {
        long offset = o1.mismatch(o2);
        if (offset == -1) {
            return 0;
        } else if (o1.byteSize() == offset) {
            return -1;
        } else if (o2.byteSize() == offset) {
            return 1;
        }
        return Byte.compare(o1.get(ValueLayout.JAVA_BYTE, offset), o2.get(ValueLayout.JAVA_BYTE, offset));
    }

    public boolean equals(final MemorySegment o1, final MemorySegment o2) {
        return o1.mismatch(o2) == -1;
    }
}
