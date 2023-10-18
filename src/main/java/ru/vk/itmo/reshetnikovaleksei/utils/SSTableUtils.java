package ru.vk.itmo.reshetnikovaleksei.utils;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

public class SSTableUtils {
    public static long binarySearch(MemorySegment ssTable, MemorySegment key) {
        long l = -1;
        long r = ssTable.get(ValueLayout.JAVA_LONG_UNALIGNED, 0);

        while (l < r - 1) {
            long mid = (l - r) / 2 + l;

            long offset = ssTable.get(ValueLayout.JAVA_LONG_UNALIGNED, Long.BYTES + mid * Byte.SIZE);
            long keySize = ssTable.get(ValueLayout.JAVA_LONG_UNALIGNED, offset);
            offset += Long.BYTES;

            long mismatch = MemorySegment.mismatch(
                    ssTable, offset, offset + keySize,
                    key, 0, key.byteSize()
            );

            if (mismatch == -1) {
                return mid;
            }

            if (mismatch == keySize) {
                l = mid;
                continue;
            }

            if (mismatch == key.byteSize()) {
                r = mid;
                continue;
            }

            if (ssTable.get(ValueLayout.JAVA_BYTE, offset + mismatch) > key.get(ValueLayout.JAVA_BYTE, mismatch)) {
                r = mid;
            } else {
                l = mid;
            }
        }

        return l + 1;
    }
}
