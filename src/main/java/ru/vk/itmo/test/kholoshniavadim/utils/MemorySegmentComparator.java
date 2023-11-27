package ru.vk.itmo.test.kholoshniavadim.utils;

import java.lang.foreign.MemorySegment;
import java.util.Comparator;

public final class MemorySegmentComparator implements Comparator<MemorySegment> {
    @Override
    public int compare(MemorySegment memorySegment1, MemorySegment memorySegment2) {
        final String string1 = StringConverterUtil.toString(memorySegment1);
        final String string2 = StringConverterUtil.toString(memorySegment2);

        if (string1 == null && string2 == null) return 0;
        if (string1 == null) return -1;
        if (string2 == null) return 1;

        return string1.compareTo(string2);
    }
}
