package ru.vk.itmo.pelogeikomakar;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.Comparator;

public class MemorySegmentComparator implements Comparator<MemorySegment> {
    @Override
    public int compare(MemorySegment o1, MemorySegment o2) {

        long o1Size = o1.byteSize();
        long o2Size = o2.byteSize();

        if (o1Size < o2Size) {
            return -1;
        } else if (o1Size > o2Size) {
            return 1;
        } else {
            long offset = -1;
            while (o1Size != 0) {
                o1Size -= 1;
                offset += 1;
                byte first = o1.get(ValueLayout.JAVA_BYTE, offset);
                byte second = o2.get(ValueLayout.JAVA_BYTE, offset);
                if (first > second) {
                    return 1;
                } else if (first < second) {
                    return -1;
                }

            }
        }
        return 0;
    }
}
