package ru.vk.itmo.dyagayalexandra;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.Comparator;

public final class MemorySegmentComparator implements Comparator<MemorySegment> {

    @Override
    public int compare(MemorySegment o1, MemorySegment o2) {
        long offset = o1.mismatch(o2);

        if (offset == -1) {
            return 0;
        }
        if (offset == o1.byteSize()) {
            return -1;
        }
        if (offset == o2.byteSize()) {
            return 1;
        }

        byte firstMemorySegmentByte = o1.get(ValueLayout.JAVA_BYTE, offset);
        byte secondMemorySegmentByte = o2.get(ValueLayout.JAVA_BYTE, offset);

        return Byte.compareUnsigned(firstMemorySegmentByte, secondMemorySegmentByte);
    }
}
