package ru.vk.itmo.plyasovklimentii;

import java.lang.foreign.MemorySegment;
import java.util.Comparator;
import java.util.Objects;

public class MemoryComparator implements Comparator<MemorySegment> {
    @Override
    public int compare(MemorySegment s1, MemorySegment s2) {
        Objects.requireNonNull(s1);
        Objects.requireNonNull(s2);

        long mismatchCount = s1.mismatch(s2);

        if (mismatchCount == 0) {
            return 0;
        } else if (mismatchCount < 0) {
            return -1;
        } else {
            return 1;
        }
    }
}
