package ru.vk.itmo.test.kachmareugene;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

public class Utils {
    public static long dumpLong(MemorySegment mapped, long value, long offset) {
        mapped.set(ValueLayout.JAVA_LONG, offset, value);
        return offset + Long.BYTES;
    }
    public static long dumpSegment(MemorySegment mapped, MemorySegment data, long offset) {
        MemorySegment.copy(data, 0, mapped, offset, data.byteSize());
        return offset + data.byteSize();
    }

}
