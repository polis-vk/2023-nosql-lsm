package ru.vk.itmo.plyasovklimentii;

import java.io.ByteArrayOutputStream;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.ArrayList;
import java.util.List;

public class VarInt {
    public static byte[] encode(long value) {
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        while (true) {
            if ((value & ~0x7FL) == 0) {
                buffer.write((int) value);
                break;
            } else {
                buffer.write(((int) value & 0x7F) | 0x80);
                value >>>= 7;
            }
        }
        return buffer.toByteArray();
    }

    public static long decode(MemorySegment segment, MutableLong position) {
        long result = 0;
        int shift = 0;
        long b;
        do {
            b = segment.getAtIndex(ValueLayout.JAVA_BYTE, position.getValue());
            result |= (b & 0x7F) << shift;
            shift += 7;
            position.increment();
        } while ((b & 0x80) != 0);
        return result;
    }
}



