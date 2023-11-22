package ru.vk.itmo.test.kachmareugene;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.Comparator;

import static java.lang.Long.min;

public class MemorySegmentComparator implements Comparator<MemorySegment> {
    @Override
    public int compare(MemorySegment segment1, MemorySegment segment2) {

        long firstDiffByte = segment1.mismatch(segment2);

        if (firstDiffByte == -1) {
            return 0;
        }

        if (firstDiffByte == min(segment1.byteSize(), segment2.byteSize())) {
            return firstDiffByte == segment1.byteSize() ? -1 : 1;
        }

        return Byte.compare(segment1.get(ValueLayout.JAVA_BYTE, firstDiffByte),
                segment2.get(ValueLayout.JAVA_BYTE, firstDiffByte));
    }
}
