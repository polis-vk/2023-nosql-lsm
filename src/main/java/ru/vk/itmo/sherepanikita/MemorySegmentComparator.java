package ru.vk.itmo.sherepanikita;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.Comparator;

public class MemorySegmentComparator implements Comparator<MemorySegment> {
    @Override
    public int compare(MemorySegment segmentOne, MemorySegment segmentTwo) {
        return compareWithOffset(segmentOne, segmentTwo, 0, segmentTwo.byteSize());
    }

    public int compareWithOffset(MemorySegment segmentOne, MemorySegment segmentTwo, long fromOffset, long toOffset) {

        long offsetInBytes = MemorySegment.mismatch(
                segmentOne, 0, segmentOne.byteSize(),
                segmentTwo, fromOffset, toOffset
        );
        long segmentOneSizeInBytes = segmentOne.byteSize();
        long segmentTwoSizeInBytes = segmentTwo.byteSize();

        if (offsetInBytes == -1) {
            return 0;
        } else if (offsetInBytes == segmentOneSizeInBytes) {
            return -1;
        } else if (offsetInBytes == segmentTwoSizeInBytes) {
            return 1;
        }
        return Byte.compare(
                segmentOne.get(ValueLayout.JAVA_BYTE, offsetInBytes),
                segmentTwo.get(ValueLayout.JAVA_BYTE, offsetInBytes)
        );
    }
}
