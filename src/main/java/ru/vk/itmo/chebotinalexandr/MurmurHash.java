package ru.vk.itmo.chebotinalexandr;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

/**
 * taken from <a href="https://github.com/apache/cassandra/blob/trunk/src/java/org/apache/cassandra/utils/MurmurHash.java#L178">...</a>
 * remade for MemorySegment
 * generates 64-bit hash
 */
public final class MurmurHash {
    public static final int DEFAULT_SEED = 104729;

    public static long[] hash64(MemorySegment key, int offset, int length) {
        return hash64(key, offset, length, DEFAULT_SEED);
    }

    @SuppressWarnings("fallthrough")
    public static long[] hash64(MemorySegment key, int offset, int length, long seed) {
        final int nblocks = length >> 4;

        long h1 = seed;
        long h2 = seed;

        long c1 = 0x87c37b91114253d5L;
        long c2 = 0x4cf5ad432745937fL;

        //----------
        // body

        for (int i = 0; i < nblocks; i++) {
            long k1 = getBlock(key, offset, i * 2 + 0);
            long k2 = getBlock(key, offset, i * 2 + 1);

            k1 *= c1;
            k1 = rotl64(k1, 31);
            k1 *= c2;
            h1 ^= k1;

            h1 = rotl64(h1, 27);
            h1 += h2;
            h1 = h1 * 5 + 0x52dce729;

            k2 *= c2;
            k2 = rotl64(k2, 33);
            k2 *= c1;
            h2 ^= k2;

            h2 = rotl64(h2, 31);
            h2 += h1;
            h2 = h2 * 5 + 0x38495ab5;
        }

        //----------
        // tail

        // Advance offset to the unprocessed tail of the data.
        offset += nblocks * 16;

        long k1 = 0;
        long k2 = 0;

        switch (length & 15) {
            case 15:
                k2 ^= ((long) key.get(ValueLayout.JAVA_BYTE, offset + 14)) << 48;
            case 14:
                k2 ^= ((long) key.get(ValueLayout.JAVA_BYTE, offset + 13)) << 40;
            case 13:
                k2 ^= ((long) key.get(ValueLayout.JAVA_BYTE, offset + 12)) << 32;
            case 12:
                k2 ^= ((long) key.get(ValueLayout.JAVA_BYTE, offset + 11)) << 24;
            case 11:
                k2 ^= ((long) key.get(ValueLayout.JAVA_BYTE, offset + 10)) << 16;
            case 10:
                k2 ^= ((long) key.get(ValueLayout.JAVA_BYTE, offset + 9)) << 8;
            case 9:
                k2 ^= ((long) key.get(ValueLayout.JAVA_BYTE, offset + 8)) << 0;
                k2 *= c2;
                k2 = rotl64(k2, 33);
                k2 *= c1;
                h2 ^= k2;
            case 8:
                k1 ^= ((long) key.get(ValueLayout.JAVA_BYTE, offset + 7)) << 56;
            case 7:
                k1 ^= ((long) key.get(ValueLayout.JAVA_BYTE, offset + 6)) << 48;
            case 6:
                k1 ^= ((long) key.get(ValueLayout.JAVA_BYTE, offset + 5)) << 40;
            case 5:
                k1 ^= ((long) key.get(ValueLayout.JAVA_BYTE, offset + 4)) << 32;
            case 4:
                k1 ^= ((long) key.get(ValueLayout.JAVA_BYTE, offset + 3)) << 24;
            case 3:
                k1 ^= ((long) key.get(ValueLayout.JAVA_BYTE, offset + 2)) << 16;
            case 2:
                k1 ^= ((long) key.get(ValueLayout.JAVA_BYTE, offset + 1)) << 8;
            case 1:
                k1 ^= key.get(ValueLayout.JAVA_BYTE, offset);
                k1 *= c1;
                k1 = rotl64(k1, 31);
                k1 *= c2;
                h1 ^= k1;
        }

        //----------
        // finalization

        h1 ^= length;
        h2 ^= length;

        h1 += h2;
        h2 += h1;

        h1 = fmix(h1);
        h2 = fmix(h2);

        h1 += h2;
        h2 += h1;

        return new long[]{h1, h2};
    }

    private static long getBlock(MemorySegment key, int offset, int index) {
        int i_8 = index << 3;
        int blockOffset = offset + i_8;
        return ((long) key.get(ValueLayout.JAVA_BYTE, blockOffset + 0) & 0xff) + (((long) key.get(ValueLayout.JAVA_BYTE, blockOffset + 1) & 0xff) << 8) +
                (((long) key.get(ValueLayout.JAVA_BYTE, blockOffset + 2) & 0xff) << 16) + (((long) key.get(ValueLayout.JAVA_BYTE, blockOffset + 3) & 0xff) << 24) +
                (((long) key.get(ValueLayout.JAVA_BYTE, blockOffset + 4) & 0xff) << 32) + (((long) key.get(ValueLayout.JAVA_BYTE, blockOffset + 5) & 0xff) << 40) +
                (((long) key.get(ValueLayout.JAVA_BYTE, blockOffset + 6) & 0xff) << 48) + (((long) key.get(ValueLayout.JAVA_BYTE, blockOffset + 7) & 0xff) << 56);
    }

    private static long rotl64(long v, int n) {
        return ((v << n) | (v >>> (64 - n)));
    }

    private static long fmix(long k) {
        k ^= k >>> 33;
        k *= 0xff51afd7ed558ccdL;
        k ^= k >>> 33;
        k *= 0xc4ceb9fe1a85ec53L;
        k ^= k >>> 33;

        return k;
    }

}
