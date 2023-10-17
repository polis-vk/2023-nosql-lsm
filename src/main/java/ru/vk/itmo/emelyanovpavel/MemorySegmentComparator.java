package ru.vk.itmo.emelyanovpavel;

import java.lang.foreign.MemorySegment;
import java.util.Comparator;

import static java.lang.foreign.ValueLayout.JAVA_BYTE;

public class MemorySegmentComparator implements Comparator<MemorySegment> {
    @Override
    public int compare(MemorySegment ms1, MemorySegment ms2) {
        long offset = ms1.mismatch(ms2);
        if (offset == -1) {
            return 0;
        }
        if (offset == ms1.byteSize()) {
            return -1;
        }
        if (offset == ms2.byteSize()) {
            return 1;
        }
        return Byte.compare(ms1.get(JAVA_BYTE, offset), ms2.get(JAVA_BYTE, offset));
    }
}
