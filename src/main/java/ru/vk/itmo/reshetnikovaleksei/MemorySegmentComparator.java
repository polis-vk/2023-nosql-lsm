package ru.vk.itmo.reshetnikovaleksei;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.Comparator;

public class MemorySegmentComparator implements Comparator<MemorySegment> {
    @Override
    public int compare(MemorySegment a, MemorySegment b) {
        var offset = a.mismatch(b);

        if (offset == -1) {
            return 0;
        } else if (offset == a.byteSize()) {
            return -1;
        } else if (offset == b.byteSize()) {
            return 1;
        } else {
            return Byte.compare(
                    a.get(ValueLayout.JAVA_BYTE, offset),
                    b.get(ValueLayout.JAVA_BYTE, offset)
            );
        }
    }
}
