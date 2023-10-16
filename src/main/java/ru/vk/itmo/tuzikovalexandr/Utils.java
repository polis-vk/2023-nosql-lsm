package ru.vk.itmo.tuzikovalexandr;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

public class Utils {
    public static MemorySegment longToMemorySegment(long l) {
        byte[] result = new byte[Long.BYTES];
        for (int i = Long.BYTES - 1; i >= 0; i--) {
            result[i] = (byte)(l & 0xFF);
            l >>= Byte.SIZE;
        }
        return MemorySegment.ofArray(result);
    }

    public static long memorySegmentToLong(MemorySegment memorySegment) {
        byte[] array = memorySegment.toArray(ValueLayout.JAVA_BYTE);

        long result = 0;
        for (int i = 0; i < Long.BYTES; i++) {
            result <<= Byte.SIZE;
            result |= (array[i] & 0xFF);
        }
        return result;
    }
}
