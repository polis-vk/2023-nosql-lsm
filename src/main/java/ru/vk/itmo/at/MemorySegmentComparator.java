package ru.vk.itmo.at;

import java.lang.foreign.MemorySegment;
import java.util.Comparator;

import static java.lang.foreign.ValueLayout.JAVA_BYTE;

public class MemorySegmentComparator implements Comparator<MemorySegment> {
    @Override
    public int compare(MemorySegment s1, MemorySegment s2) {
        long mismatch = s1.mismatch(s2);
        if (mismatch == -1) {
            return 0;
        }
        if (mismatch == s1.byteSize()) {
            return -1;
        }
        if (mismatch == s2.byteSize()) {
            return 1;
        }
        return Byte.compare(s1.get(JAVA_BYTE, mismatch), s2.get(JAVA_BYTE, mismatch));
    }
}
