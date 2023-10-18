package ru.vk.itmo.kovalchukvladislav;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.Comparator;

public class MemorySegmentComparator implements Comparator<MemorySegment> {
    public static final MemorySegmentComparator INSTANCE = new MemorySegmentComparator();
    private static final ValueLayout.OfByte VALUE_LAYOUT = ValueLayout.JAVA_BYTE;

    @Override
    public int compare(MemorySegment a, MemorySegment b) {
        long diffIndex = a.mismatch(b);
        if (diffIndex == -1) {
            return 0;
        } else if (diffIndex == a.byteSize()) {
            return -1;
        } else if (diffIndex == b.byteSize()) {
            return 1;
        }

        byte byteA = a.getAtIndex(VALUE_LAYOUT, diffIndex);
        byte byteB = b.getAtIndex(VALUE_LAYOUT, diffIndex);
        return Byte.compare(byteA, byteB);
    }
}
