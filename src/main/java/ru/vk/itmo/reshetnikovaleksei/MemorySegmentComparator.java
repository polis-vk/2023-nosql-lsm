package ru.vk.itmo.reshetnikovaleksei;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.Comparator;

public final class MemorySegmentComparator implements Comparator<MemorySegment> {
    private static MemorySegmentComparator instance;

    private MemorySegmentComparator() {
    }

    public static synchronized MemorySegmentComparator getInstance() {
        if (instance == null) {
            instance = new MemorySegmentComparator();
        }

        return instance;
    }

    @Override
    public int compare(MemorySegment a, MemorySegment b) {
        var offset = a.mismatch(b);

        if (offset == -1) {
            return 0;
        } else if (offset == a.byteSize()) {
            return -1;
        } else if (offset == b.byteSize()) {
            return 1;
        } else {
            return Byte.compare(
                    a.get(ValueLayout.JAVA_BYTE, offset),
                    b.get(ValueLayout.JAVA_BYTE, offset)
            );
        }
    }

    public int compare(MemorySegment a, MemorySegment b, long fromOffset, long toOffset) {
        long mismatch = MemorySegment.mismatch(
                b, fromOffset, toOffset,
                a, 0, a.byteSize()
        );

        if (mismatch == -1) {
            return 0;
        } else if (mismatch == a.byteSize()) {
            return -1;
        } else if (mismatch == b.byteSize()) {
            return 1;
        }

        return Byte.compare(
                a.get(ValueLayout.JAVA_BYTE, mismatch),
                b.get(ValueLayout.JAVA_BYTE, mismatch + fromOffset)
        );
    }
}
