package ru.vk.itmo.belonogovnikolay;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.Comparator;

public class MemorySegmentComparator implements Comparator<MemorySegment> {

    @Override
    public int compare(MemorySegment prevSegment, MemorySegment nextSegment) {

        long offset = prevSegment.mismatch(nextSegment);

        if (offset == nextSegment.byteSize()) {
            return 1;
        } else if (offset == prevSegment.byteSize()) {
            return -1;
        } else if (offset == -1) {
            return 0;
        }

        return Byte.compare(prevSegment.get(ValueLayout.JAVA_BYTE, offset),
                nextSegment.get(ValueLayout.JAVA_BYTE, offset));
    }
}
