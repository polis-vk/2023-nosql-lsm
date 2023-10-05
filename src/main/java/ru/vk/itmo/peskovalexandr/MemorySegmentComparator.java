package ru.vk.itmo.peskovalexandr;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.Comparator;

public class MemorySegmentComparator implements Comparator<MemorySegment> {

    @Override
    public int compare(MemorySegment o1, MemorySegment o2) {
        long mismatchOffset = o1.mismatch(o2);
        if (mismatchOffset == -1) {
            return 0;
        }
        if (mismatchOffset == o1.byteSize()) {
            return -1;
        }
        if (mismatchOffset == o2.byteSize()) {
            return 1;
        }
        return o1.get(ValueLayout.JAVA_BYTE, mismatchOffset) - o2.get(ValueLayout.JAVA_BYTE, mismatchOffset);
    }
}
