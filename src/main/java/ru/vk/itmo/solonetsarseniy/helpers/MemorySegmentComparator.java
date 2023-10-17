package ru.vk.itmo.solonetsarseniy.helpers;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.Comparator;

public class MemorySegmentComparator implements Comparator<MemorySegment> {
    @Override
    public int compare(MemorySegment first, MemorySegment second) {
        long mismatch = first.mismatch(second);
        if (mismatch == -1) {
            return 0;
        }

        if (mismatch == first.byteSize()) {
            return -1;
        }

        if (mismatch == second.byteSize()) {
            return 1;
        }
        byte b1 = first.get(ValueLayout.JAVA_BYTE, mismatch);
        byte b2 = second.get(ValueLayout.JAVA_BYTE, mismatch);
        return Byte.compare(b1, b2);
    }
}
