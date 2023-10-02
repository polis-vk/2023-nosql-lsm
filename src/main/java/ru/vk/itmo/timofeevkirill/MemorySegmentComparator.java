package ru.vk.itmo.timofeevkirill;

import java.lang.foreign.MemorySegment;
import java.util.Comparator;

public class MemorySegmentComparator implements Comparator<MemorySegment> {
    @Override
    public int compare(MemorySegment segment1, MemorySegment segment2) {
        byte[] byteArray1 = (byte[]) segment1.heapBase().orElseThrow();
        byte[] byteArray2 = (byte[]) segment2.heapBase().orElseThrow();

        int minLength = Math.min(byteArray1.length, byteArray2.length);
        for (int i = 0; i < minLength; i++) {
            int cmp = Byte.compare(byteArray1[i], byteArray2[i]);
            if (cmp != 0) {
                return cmp;
            }
        }
        return Integer.compare(byteArray1.length, byteArray2.length);
    }
}
