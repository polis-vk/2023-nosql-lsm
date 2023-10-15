package ru.vk.itmo.prokopyevnikita;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.Comparator;

public class MemorySegmentComparator implements Comparator<MemorySegment> {

    public static final Comparator<MemorySegment> INSTANCE = new MemorySegmentComparator();

    private MemorySegmentComparator() {
    }

    @Override
    public int compare(MemorySegment o1, MemorySegment o2) {
        long relativeOffset = o1.mismatch(o2);
        if (relativeOffset == -1) {
            return 0;
        } else if (relativeOffset == o1.byteSize()) {
            return -1;
        } else if (relativeOffset == o2.byteSize()) {
            return 1;
        }
        return Byte.compare(
                o1.get(ValueLayout.JAVA_BYTE, relativeOffset),
                o2.get(ValueLayout.JAVA_BYTE, relativeOffset)
        );
    }
}
