package ru.vk.itmo.gamzatgadzhimagomedov;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.Comparator;

public class MemorySegmentComparator implements Comparator<MemorySegment> {

    @Override
    public int compare(MemorySegment o1, MemorySegment o2) {

        long index = o1.mismatch(o2);
        if (index == o1.byteSize()) {
            return -1;
        }
        if (index == o2.byteSize()) {
            return 1;
        }
        if (index == -1) {
            return 0;
        }

        return Byte.compare(
                o1.get(ValueLayout.JAVA_BYTE, index),
                o2.get(ValueLayout.JAVA_BYTE, index)
        );
    }
}
