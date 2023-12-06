package ru.vk.itmo.bandurinvladislav;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteBuffer;

// I took this implementation from https://github.com/apache/commons-codec/blob/master/src/main/java/org/apache/commons/codec/digest/MurmurHash3.java
public final class MurmurHash3 {
    public static final int DEFAULT_SEED = 104729;
    private static final long C1 = 0x87c37b91114253d5L;
    private static final long C2 = 0x4cf5ad432745937fL;
    private static final int R1 = 31;
    private static final int R2 = 27;
    private static final int R3 = 33;
    private static final int M = 5;

    private static final int N1 = 0x52dce729;

    private static final int N2 = 0x38495ab5;


    /**
     * Performs the final avalanche mix step of the 64-bit hash function {@code MurmurHash3_x64_128}.
     *
     * @param hash The current hash
     * @return The final hash
     */
    private static long fmix64(long hash) {
        hash ^= hash >>> 33;
        hash *= 0xff51afd7ed558ccdL;
        hash ^= hash >>> 33;
        hash *= 0xc4ceb9fe1a85ec53L;
        hash ^= hash >>> 33;
        return hash;
    }

    public static long[] hash128x64(final ByteBuffer data, final int offset, final int length, final int seed, long[] indexes) {
        // Use an unsigned 32-bit integer as the seed
        return hash128x64Internal(data, offset, length, seed & 0xffffffffL, indexes);
    }

    private static long getLittleEndianLong(final ByteBuffer data, final int index) {
        return (long) data.get(index) & 0xff |
                ((long) data.get(index + 1) & 0xff) << 8 |
                ((long) data.get(index + 2) & 0xff) << 16 |
                ((long) data.get(index + 3) & 0xff) << 24 |
                ((long) data.get(index + 4) & 0xff) << 32 |
                ((long) data.get(index + 5) & 0xff) << 40 |
                ((long) data.get(index + 6) & 0xff) << 48 |
                ((long) data.get(index + 7) & 0xff) << 56;
    }

    private static long[] hash128x64Internal(final ByteBuffer data, final int offset, final int length, final long seed, long[] indexes) {
        long h1 = seed;
        long h2 = seed;
        final int nblocks = length >> 4;

        // body
        for (int i = 0; i < nblocks; i++) {
            final int index = offset + (i << 4);
            long k1 = getLittleEndianLong(data, index);
            long k2 = getLittleEndianLong(data, index + 8);

            // mix functions for k1
            k1 *= C1;
            k1 = Long.rotateLeft(k1, R1);
            k1 *= C2;
            h1 ^= k1;
            h1 = Long.rotateLeft(h1, R2);
            h1 += h2;
            h1 = h1 * M + N1;

            // mix functions for k2
            k2 *= C2;
            k2 = Long.rotateLeft(k2, R3);
            k2 *= C1;   
            h2 ^= k2;
            h2 = Long.rotateLeft(h2, R1);
            h2 += h1;
            h2 = h2 * M + N2;
        }

        // tail
        long k1 = 0;
        long k2 = 0;
        final int index = offset + (nblocks << 4);
        switch (offset + length - index) {
            case 15:
                k2 ^= ((long) data.get(index + 14) & 0xff) << 48;
            case 14:
                k2 ^= ((long) data.get(index + 13) & 0xff) << 40;
            case 13:
                k2 ^= ((long) data.get(index + 12) & 0xff) << 32;
            case 12:
                k2 ^= ((long) data.get(index + 11) & 0xff) << 24;
            case 11:
                k2 ^= ((long) data.get(index + 10) & 0xff) << 16;
            case 10:
                k2 ^= ((long) data.get(index + 9) & 0xff) << 8;
            case 9:
                k2 ^= data.get(index + 8) & 0xff;
                k2 *= C2;
                k2 = Long.rotateLeft(k2, R3);
                k2 *= C1;
                h2 ^= k2;

            case 8:
                k1 ^= ((long) data.get(index + 7) & 0xff) << 56;
            case 7:
                k1 ^= ((long) data.get(index + 6) & 0xff) << 48;
            case 6:
                k1 ^= ((long) data.get(index + 5) & 0xff) << 40;
            case 5:
                k1 ^= ((long) data.get(index + 4) & 0xff) << 32;
            case 4:
                k1 ^= ((long) data.get(index + 3) & 0xff) << 24;
            case 3:
                k1 ^= ((long) data.get(index + 3) & 0xff) << 16;
            case 2:
                k1 ^= ((long) data.get(index + 2) & 0xff) << 8;
            case 1:
                k1 ^= data.get(index) & 0xff;
                k1 *= C1;
                k1 = Long.rotateLeft(k1, R1);
                k1 *= C2;
                h1 ^= k1;
        }

        // finalization
        h1 ^= length;
        h2 ^= length;

        h1 += h2;
        h2 += h1;

        h1 = fmix64(h1);
        h2 = fmix64(h2);

        h1 += h2;
        h2 += h1;

        indexes[0] = h1;
        indexes[1] = h2;

        return indexes;
    }

    /**
     * No instance methods.
     */
    private MurmurHash3() {
    }

}