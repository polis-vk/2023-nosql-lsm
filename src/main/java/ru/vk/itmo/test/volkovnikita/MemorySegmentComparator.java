package ru.vk.itmo.test.volkovnikita;

import java.lang.foreign.MemorySegment;
import java.util.Comparator;

public class MemorySegmentComparator implements Comparator<MemorySegment> {

    @Override
    public int compare(MemorySegment o1, MemorySegment o2) {
        return o1.asByteBuffer().compareTo(o2.asByteBuffer());
    }
}
