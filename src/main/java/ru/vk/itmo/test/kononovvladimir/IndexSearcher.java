package ru.vk.itmo.test.kononovvladimir;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

public class IndexSearcher{
    MemorySegment memorySegment;
    public IndexSearcher(MemorySegment memorySegment) {
        this.memorySegment = memorySegment;
    }

    public long getSslSize() {
        return memorySegment.get(ValueLayout.JAVA_LONG_UNALIGNED, 0);
    }

    public long getNumberNulls() {
        return memorySegment.get(ValueLayout.JAVA_LONG_UNALIGNED, Long.BYTES);
    }

    public long getDataOffset(long ind) {
        return memorySegment.get(ValueLayout.JAVA_LONG_UNALIGNED, Long.BYTES * 3L * ind + Long.BYTES + Long.BYTES + Long.BYTES);
    }

    public long getKeyOffset(long ind) {
        return memorySegment.get(ValueLayout.JAVA_LONG_UNALIGNED, Long.BYTES * 3L * ind + Long.BYTES + Long.BYTES + Long.BYTES + Long.BYTES);
    }
}
