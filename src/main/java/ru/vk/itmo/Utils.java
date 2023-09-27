package ru.vk.itmo;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.Comparator;

public final class Utils {

    public static Comparator<MemorySegment> memorySegmentComparator = (o1, o2) -> {
        long size1 = o1.byteSize();
        long size2 = o2.byteSize();
        int i1 = 0;
        int i2 = 0;
        int compare = Long.compare(size1, size2);

        while (i1 != size1 && i2 != size2) {
            compare = Byte.compare(o1.get(ValueLayout.JAVA_BYTE, i1), o2.get(ValueLayout.JAVA_BYTE, i2));
            if (compare != 0) {
                return compare;
            }
            i1++;
            i2++;
        }

        return compare;
    };

    private Utils() {
    }
}
