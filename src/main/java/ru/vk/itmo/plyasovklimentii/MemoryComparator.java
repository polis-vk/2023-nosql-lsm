package ru.vk.itmo.plyasovklimentii;

import java.lang.foreign.MemorySegment;
import java.util.Comparator;
import java.util.Objects;

import static java.lang.foreign.ValueLayout.JAVA_BYTE;

public class MemoryComparator implements Comparator<MemorySegment> {
    @Override
    public int compare(MemorySegment s1, MemorySegment s2) {
        Objects.requireNonNull(s1);
        Objects.requireNonNull(s2);

        long mismatchOffset = s1.mismatch(s2);

        if (mismatchOffset < 0) {
            return 0;
        }
        if (mismatchOffset == s1.byteSize()) {
            return -1;
        }
        if (mismatchOffset == s2.byteSize()) {
            return 1;
        }

        return Byte.compare(s1.get(JAVA_BYTE, mismatchOffset), s2.get(JAVA_BYTE, mismatchOffset));
    }
}
