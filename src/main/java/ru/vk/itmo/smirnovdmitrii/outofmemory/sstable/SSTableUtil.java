package ru.vk.itmo.smirnovdmitrii.outofmemory.sstable;

public final class SSTableUtil {
    private SSTableUtil() {
    }

    public static long tombstone(final long value) {
        return value | 1L << 63;
    }

    public static long normalize(final long value) {
        return value & ~(1L << 63);
    }
}
