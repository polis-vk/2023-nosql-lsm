package ru.vk.itmo.tveritinalexandr;

import java.lang.foreign.MemorySegment;
import java.util.Comparator;

import static java.lang.foreign.ValueLayout.JAVA_BYTE;

public class MemorySegmentComparator implements Comparator<MemorySegment> {

    public static final MemorySegmentComparator INSTANCE = new MemorySegmentComparator();

    @Override
    public int compare(MemorySegment o1, MemorySegment o2) {
        var offset = o1.mismatch(o2);

        if (offset == -1) return 0;
        if (offset == o1.byteSize()) return -1;
        if (offset == o2.byteSize()) return 1;
        if (o1.get(JAVA_BYTE, offset) > o2.get(JAVA_BYTE, offset)) return 1;
        else return -1;
    }
}
