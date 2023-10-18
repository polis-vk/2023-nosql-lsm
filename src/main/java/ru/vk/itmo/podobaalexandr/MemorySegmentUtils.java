package ru.vk.itmo.podobaalexandr;

import ru.vk.itmo.BaseEntry;
import ru.vk.itmo.Entry;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

public class MemorySegmentUtils {

    private MemorySegmentUtils() {

    }

    public static int compare(MemorySegment o1, MemorySegment o2) {
        return compareSegments(o1, o1.byteSize(), o2, 0, o2.byteSize());
    }

    public static int compareSegments(MemorySegment src, long srcEnd,
                                      MemorySegment dest, long destOffset, long destEnd) {
        long destLength = destEnd - destOffset;

        int sizeDiff = Long.compare(srcEnd, destLength);

        if (srcEnd == 0 || destLength == 0) {
            return sizeDiff;
        }

        long mismatch = MemorySegment.mismatch(src, 0, srcEnd, dest, destOffset, destEnd);

        if (mismatch == destLength || mismatch == srcEnd) {
            return sizeDiff;
        }

        return mismatch == -1
                ? 0
                : Byte.compare(src.get(ValueLayout.JAVA_BYTE, mismatch),
                dest.get(ValueLayout.JAVA_BYTE, destOffset + mismatch));
    }

    public static Entry<MemorySegment> getKeyValueFromOffset(MemorySegment page,
                                                             long offset, long keySize, long keysSize) {
        long offsetLocal = offset;

        MemorySegment key = page.asSlice(offsetLocal, keySize);
        offsetLocal += keySize;

        long offsetToV = page.get(ValueLayout.JAVA_LONG_UNALIGNED, offsetLocal);

        if (offsetToV == 0) {
            return new BaseEntry<>(key, null);
        }

        offsetLocal += 2 * Long.BYTES + Byte.BYTES;
        MemorySegment value = null;
        long size = 0;
        while (size <= 0 && offsetLocal < keysSize) {
            long keySizeNextKey = page.get(ValueLayout.JAVA_LONG_UNALIGNED, offsetLocal);
            offsetLocal += Long.BYTES + keySizeNextKey;

            size = page.get(ValueLayout.JAVA_LONG_UNALIGNED, offsetLocal) - offsetToV;
            if (size > 0) {
                value = page.asSlice(offsetToV, size);
            }
            offsetLocal += 2 * Long.BYTES + Byte.BYTES;
        }

        value = value == null ? page.asSlice(offsetToV) : value;

        return new BaseEntry<>(key, value);
    }

}
