package ru.vk.itmo.proninvalentin.comparators;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.Comparator;

public class MemorySegmentComparator implements Comparator<MemorySegment> {
    @Override
    public int compare(MemorySegment ms1, MemorySegment ms2) {
        if (ms1 == null && ms2 == null) {
            return 0;
        } else if (ms1 != null && ms2 == null) {
            return -1;
        } else if (ms1 == null) {
            return 1;
        }

        long mismatchOffset = ms1.mismatch(ms2);
        if (mismatchOffset == -1) {
            return 0;
        } else if (mismatchOffset == ms1.byteSize()) {
            return -1;
        } else if (mismatchOffset == ms2.byteSize()) {
            return 1;
        }

        return Byte.compare(
                ms1.get(ValueLayout.JAVA_BYTE, mismatchOffset),
                ms2.get(ValueLayout.JAVA_BYTE, mismatchOffset));
    }
}
