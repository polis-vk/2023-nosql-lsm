package ru.vk.itmo.mozzhevilovdanil;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.Comparator;

public class DatabaseUtils {
    public static final Comparator<MemorySegment> comparator = DatabaseUtils::compare;

    private DatabaseUtils() {
    }

    static long binSearch(MemorySegment index, MemorySegment readPage, MemorySegment key) {
        long left = 0;
        long right = index.byteSize() / Long.BYTES;
        while (left < right) {
            long mid = (left + (right - left) / 2) * Long.BYTES;
            long offset = index.get(ValueLayout.JAVA_LONG_UNALIGNED, mid);
            long compareResult = compareInPlace(readPage, offset, key);
            if (compareResult == 0) {
                return mid;
            }
            if (compareResult < 0) {
                left = mid / Long.BYTES + 1;
            } else {
                right = mid / Long.BYTES;
            }
        }
        return left * Long.BYTES;
    }

    public static long compareInPlace(MemorySegment readPage, long offsetOfCompareValue, MemorySegment key) {

        long keySize = readPage.get(ValueLayout.JAVA_LONG_UNALIGNED, offsetOfCompareValue);
        offsetOfCompareValue += 2 * Long.BYTES;

        long mismatch = MemorySegment.mismatch(
                readPage,
                offsetOfCompareValue,
                offsetOfCompareValue + key.byteSize(),
                key,
                0,
                key.byteSize()
        );

        if (mismatch == -1) {
            return Long.compare(keySize, key.byteSize());
        }

        if (mismatch == keySize) {
            return -1;
        }

        if (mismatch == key.byteSize()) {
            return 1;
        }

        byte b1 = readPage.get(ValueLayout.JAVA_BYTE, offsetOfCompareValue + mismatch);
        byte b2 = key.get(ValueLayout.JAVA_BYTE, mismatch);
        return Byte.compare(b1, b2);
    }

    private static int compare(MemorySegment lhs, MemorySegment rhs) {
        long mismatch = lhs.mismatch(rhs);
        if (mismatch == -1) {
            return 0;
        }

        if (mismatch == lhs.byteSize()) {
            return -1;
        }

        if (mismatch == rhs.byteSize()) {
            return 1;
        }

        byte b1 = lhs.get(ValueLayout.JAVA_BYTE, mismatch);
        byte b2 = rhs.get(ValueLayout.JAVA_BYTE, mismatch);
        return Byte.compare(b1, b2);
    }
}
