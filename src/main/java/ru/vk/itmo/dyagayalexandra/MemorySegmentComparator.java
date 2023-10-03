package ru.vk.itmo.dyagayalexandra;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.Comparator;

public final class MemorySegmentComparator implements Comparator<MemorySegment> {

    @Override
    public int compare(MemorySegment memorySegment1, MemorySegment memorySegment2) {
        long offset = memorySegment1.mismatch(memorySegment2);

        if (offset == -1) {
            return 0;
        }
        if (offset == memorySegment1.byteSize()) {
            return -1;
        }
        if (offset == memorySegment2.byteSize()) {
            return 1;
        }

        byte firstMemorySegmentByte = memorySegment1.get(ValueLayout.JAVA_BYTE, offset);
        byte secondMemorySegmentByte = memorySegment2.get(ValueLayout.JAVA_BYTE, offset);

        return Byte.compare(firstMemorySegmentByte, secondMemorySegmentByte);
    }
}
