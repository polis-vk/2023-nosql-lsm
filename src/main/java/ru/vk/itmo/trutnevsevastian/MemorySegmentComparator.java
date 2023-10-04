package ru.vk.itmo.trutnevsevastian;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.Comparator;

public class MemorySegmentComparator implements Comparator<MemorySegment> {
    @Override
    public int compare(MemorySegment o1, MemorySegment o2) {
        long lim = Math.min(o1.byteSize(), o2.byteSize());
        long k = MemorySegment.mismatch(o1, 0, lim, o2, 0, lim);
        if (k >= 0) {
            return o1.get(ValueLayout.JAVA_BYTE, k) - o2.get(ValueLayout.JAVA_BYTE, k);
        }
        if (o1.byteSize() == o2.byteSize()) {
            return 0;
        }
        return o1.byteSize() < o2.byteSize() ? -1 : 1;
    }
}
