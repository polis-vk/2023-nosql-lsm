package ru.vk.itmo.tuzikovalexandr;

import java.lang.foreign.MemorySegment;
import java.util.Arrays;
import java.util.Comparator;

public class MemorySegmentComparator implements Comparator<MemorySegment> {

    @Override
    public int compare(MemorySegment o1, MemorySegment o2) {
        return Arrays.compareUnsigned(o1.asByteBuffer().array(), o2.asByteBuffer().array());
    }
}
