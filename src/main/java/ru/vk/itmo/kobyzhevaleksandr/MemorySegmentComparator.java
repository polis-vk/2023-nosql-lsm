package ru.vk.itmo.kobyzhevaleksandr;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.Comparator;

public class MemorySegmentComparator implements Comparator<MemorySegment> {

    @Override
    public int compare(MemorySegment memorySegment1, MemorySegment memorySegment2) {
        long offset = memorySegment1.mismatch(memorySegment2);
        if (offset == -1) {
            return 0;
        } else if (offset == memorySegment1.byteSize()) {
            return -1;
        } else if (offset == memorySegment2.byteSize()) {
            return 1;
        }
        return Byte.compare(memorySegment1.get(ValueLayout.JAVA_BYTE, offset),
            memorySegment2.get(ValueLayout.JAVA_BYTE, offset));
    }
}
