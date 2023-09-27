package ru.vk.itmo.danilinandrew;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.Comparator;

public class MemorySegmentComparator implements Comparator<MemorySegment> {

    @Override
    public int compare(MemorySegment o1, MemorySegment o2) {
        int offset = 0;

        while (offset < o1.byteSize() && offset < o2.byteSize()) {
            byte byte1 = o1.get(ValueLayout.JAVA_BYTE, offset);
            byte byte2 = o2.get(ValueLayout.JAVA_BYTE, offset);

            int compareRes = Byte.compare(byte1, byte2);

            if (compareRes == 0) {
                offset++;
            } else {
                return compareRes;
            }
        }

        return Long.compare(o1.byteSize(), o2.byteSize());
    }
}
