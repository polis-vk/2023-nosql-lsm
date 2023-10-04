package ru.vk.itmo.test.emelyanovvitaliy;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.Comparator;

public class MemSegmentComparator implements Comparator<MemorySegment> {

    @Override
    public int compare(MemorySegment o1, MemorySegment o2) {
        long mismatch = o1.mismatch(o2);
        if (mismatch == -1) {
            return 0;
        }
        if (mismatch == o1.byteSize()) {
            return -1;
        }
        if (mismatch == o2.byteSize()) {
            return 1;
        }
        return Byte.compare(
                o1.getAtIndex(ValueLayout.JAVA_BYTE, mismatch),
                o2.getAtIndex(ValueLayout.JAVA_BYTE, mismatch)
        );
    }
}
