package ru.vk.itmo.test.kachmareugene;

import java.lang.foreign.MemorySegment;

public class MemSegComparatorNull extends MemorySegmentComparator {
    @Override
    public int compare(MemorySegment segment1, MemorySegment segment2) {
        if (segment1 == null && segment2 == null) {
            throw new IllegalArgumentException("Incomparable null and null");
        }
        if (segment1 == null) {
            return -1;
        }
        if (segment2 == null) {
            return -1;
        }

        return super.compare(segment1, segment2);
    }
}
