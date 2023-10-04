package ru.vk.itmo.bazhenovkirill;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.Comparator;

public final class MemorySegmentComparator implements Comparator<MemorySegment> {
    private static MemorySegmentComparator instance;

    private MemorySegmentComparator() {
    }

    public static MemorySegmentComparator getInstance() {
        if (instance == null) {
            synchronized (MemorySegmentComparator.class) {
                if (instance == null) {
                    instance = new MemorySegmentComparator();
                }
            }
        }
        return instance;
    }

    @Override
    public int compare(MemorySegment ms1, MemorySegment ms2) {
        long mismatch = ms1.mismatch(ms2);
        if (mismatch == -1) {
            return 0;
        } else {
            if (ms2.byteSize() == mismatch) {
                return 1;
            }
            if (ms1.byteSize() == mismatch) {
                return -1;
            }
            return Byte.compare(
                    ms1.get(ValueLayout.JAVA_BYTE, mismatch),
                    ms2.get(ValueLayout.JAVA_BYTE, mismatch));
        }
    }
}
