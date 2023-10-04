package ru.vk.itmo.savkovskiyegor;

import java.lang.foreign.MemorySegment;
import java.util.Comparator;

import static java.lang.foreign.ValueLayout.JAVA_BYTE;

class MemorySegmentComparator implements Comparator<MemorySegment> {

    @Override
    public int compare(MemorySegment s1, MemorySegment s2) {
        long mismatch = s1.mismatch(s2);

        // segments are equal
        if (mismatch == -1) {
            return 0;
        }
        // first segment is smaller than second
        if (mismatch == s1.byteSize()) {
            return -1;
        }
        // second segment is smaller than first
        if (mismatch == s2.byteSize()) {
            return 1;
        }

        return Byte.compare(s1.get(JAVA_BYTE, mismatch), s2.get(JAVA_BYTE, mismatch));
    }
}
