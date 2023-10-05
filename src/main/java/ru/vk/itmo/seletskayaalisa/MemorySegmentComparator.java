package ru.vk.itmo.seletskayaalisa;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.Comparator;

public class MemorySegmentComparator implements Comparator<MemorySegment> {

    @Override
    public int compare(MemorySegment ms1, MemorySegment ms2) {
        long offset = ms1.mismatch(ms2);
        if (offset == ms1.byteSize()) {
            return -1;
        }
        if (offset == ms2.byteSize()) {
            return 1;
        }
        if (offset == -1) {
            return 0;
        }
        return Byte.compare(ms1.get(ValueLayout.JAVA_BYTE, offset),
                ms2.get(ValueLayout.JAVA_BYTE, offset));
    }
}
