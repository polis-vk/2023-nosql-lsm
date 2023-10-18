package ru.vk.itmo.test.volkovnikita;

import java.lang.foreign.MemorySegment;
import java.util.Comparator;

import static java.lang.foreign.ValueLayout.JAVA_BYTE;

public class MemorySegmentComparator implements Comparator<MemorySegment> {

    @Override
    public int compare(MemorySegment o1, MemorySegment o2) {
        long mismatchOffSet = o1.mismatch(o2);
        if (mismatchOffSet == o2.byteSize()) {
            return 1;
        }
        if (mismatchOffSet == o1.byteSize()) {
            return -1;
        }
        if (mismatchOffSet == -1) {
            return 0;
        }

        return o1.get(JAVA_BYTE, mismatchOffSet) - o2.get(JAVA_BYTE, mismatchOffSet);
    }
}
