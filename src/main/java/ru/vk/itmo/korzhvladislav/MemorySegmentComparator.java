package ru.vk.itmo.korzhvladislav;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.Comparator;

public class MemorySegmentComparator implements Comparator<MemorySegment> {
    @Override
    public int compare(MemorySegment segFirst, MemorySegment segSecond) {
        long offSetMismatch = segFirst.mismatch(segSecond);
        if (offSetMismatch == -1) {
            return 0;
        } else if (offSetMismatch == segFirst.byteSize()) {
            return -1;
        } else if (offSetMismatch == segSecond.byteSize()) {
            return 1;
        }
        return Byte.compare(segFirst.getAtIndex(ValueLayout.JAVA_BYTE, offSetMismatch),
                segSecond.getAtIndex(ValueLayout.JAVA_BYTE, offSetMismatch));
    }
}
