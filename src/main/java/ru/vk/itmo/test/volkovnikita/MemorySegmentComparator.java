package ru.vk.itmo.test.volkovnikita;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.Comparator;

public class MemorySegmentComparator implements Comparator<MemorySegment> {

    @Override
    public int compare(MemorySegment o1, MemorySegment o2) {
        long mismatchOffSet = o1.mismatch(o2);
        if (mismatchOffSet == -1) {
            return 0;
        }
        if (o1.byteSize() == mismatchOffSet) {
            return -1;
        }
        if (o2.byteSize() == mismatchOffSet) {
            return 1;
        }
        return Byte.compare(o1.get(ValueLayout.JAVA_BYTE, mismatchOffSet), o2.get(ValueLayout.JAVA_BYTE, mismatchOffSet));
    }
}