package ru.vk.itmo.pelogeikomakar;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.Comparator;

public class MemorySegmentComparator implements Comparator<MemorySegment> {
    @Override
    public int compare(MemorySegment o1, MemorySegment o2) {

        long o1Size = o1.byteSize();
        long o2Size = o2.byteSize();

        if (o1Size == 0 && o2Size == 0) {
            return 0;
        } else if (o1Size == 0) {
            return -1;
        } else if (o2Size == 0) {
            return 1;
        }

        long mismatchOffset = o1.mismatch(o2);

        if (mismatchOffset < 0) {
            if (o1Size < o2Size) {
                return -1;
            } else if (o1Size > o2Size) {
                return 1;
            }
            return 0;
        }

        byte first = o1.get(ValueLayout.JAVA_BYTE, mismatchOffset);
        byte second = o2.get(ValueLayout.JAVA_BYTE, mismatchOffset);
        if (first != second) {
            return first - second;
        }

        return 0;
    }
}
