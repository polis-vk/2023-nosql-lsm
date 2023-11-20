package ru.vk.itmo.pologovnikita;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.Comparator;

public class MemorySegmentComparator implements Comparator<MemorySegment> {
    @Override
    public int compare(MemorySegment o1, MemorySegment o2) {
        var offset = o1.mismatch(o2);
        if (offset == -1) {
            return 0;
        }
        if (offset == o1.byteSize()) {
            return -1;
        }
        if (offset == o2.byteSize()) {
            return 1;
        }
        byte b1 = o1.get(ValueLayout.JAVA_BYTE, offset);
        byte b2 = o2.get(ValueLayout.JAVA_BYTE, offset);
        return Byte.compare(b1, b2);
    }
}
