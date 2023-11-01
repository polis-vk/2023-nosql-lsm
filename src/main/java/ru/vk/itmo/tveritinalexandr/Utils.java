package ru.vk.itmo.tveritinalexandr;

import java.lang.foreign.MemorySegment;

public final class Utils {

    private Utils() {
    }

    public static MemorySegment slice(MemorySegment page, long start, long end) {
        return page.asSlice(start, end - start);
    }

    public static long tombstone(long offset) {
        return 1L << 63 | offset;
    }

    public static long normalize(long value) {
        return value & ~(1L << 63);
    }

}
