package ru.vk.itmo.chebotinalexandr;

import sun.misc.Unsafe;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * taken from <a href="https://github.com/apache/cassandra/blob/trunk/src/java/org/apache/cassandra/utils/MurmurHash.java#L178">...</a>.
 * remade for MemorySegment
 * generates 64-bit hash
 */
public final class MurmurHash {
    public static final int DEFAULT_SEED = 104729;

    private MurmurHash() {

    }

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
            long k1 = getBlock(key, offset, i);

            k1 *= c1;
            k1 = rotl64(k1, 31);
            k1 *= c2;
            h1 ^= k1;

            h1 = rotl64(h1, 27);
            h1 += h2;
            h1 = h1 * 5 + 0x52dce729;

            long k2 = getBlock(key, offset, i + 8);

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
        long advancedOffset = (long) offset + nblocks * 16;

        long k1 = 0;
        long k2 = 0;

        switch (length & 15) {
            case 15:
                k2 ^= ((long) key.get(ValueLayout.JAVA_BYTE, advancedOffset + 14)) << 48;
            case 14:
                k2 ^= ((long) key.get(ValueLayout.JAVA_BYTE, advancedOffset + 13)) << 40;
            case 13:
                k2 ^= ((long) key.get(ValueLayout.JAVA_BYTE, advancedOffset + 12)) << 32;
            case 12:
                k2 ^= ((long) key.get(ValueLayout.JAVA_BYTE, advancedOffset + 11)) << 24;
            case 11:
                k2 ^= ((long) key.get(ValueLayout.JAVA_BYTE, advancedOffset + 10)) << 16;
            case 10:
                k2 ^= ((long) key.get(ValueLayout.JAVA_BYTE, advancedOffset + 9)) << 8;
            case 9:
                k2 ^= key.get(ValueLayout.JAVA_BYTE, advancedOffset + 8);
                k2 *= c2;
                k2 = rotl64(k2, 33);
                k2 *= c1;
                h2 ^= k2;
            case 8:
                k1 ^= ((long) key.get(ValueLayout.JAVA_BYTE, advancedOffset + 7)) << 56;
            case 7:
                k1 ^= ((long) key.get(ValueLayout.JAVA_BYTE, advancedOffset + 6)) << 48;
            case 6:
                k1 ^= ((long) key.get(ValueLayout.JAVA_BYTE, advancedOffset + 5)) << 40;
            case 5:
                k1 ^= ((long) key.get(ValueLayout.JAVA_BYTE, advancedOffset + 4)) << 32;
            case 4:
                k1 ^= ((long) key.get(ValueLayout.JAVA_BYTE, advancedOffset + 3)) << 24;
            case 3:
                k1 ^= ((long) key.get(ValueLayout.JAVA_BYTE, advancedOffset + 2)) << 16;
            case 2:
                k1 ^= ((long) key.get(ValueLayout.JAVA_BYTE, advancedOffset + 1)) << 8;
            case 1:
                k1 ^= key.get(ValueLayout.JAVA_BYTE, advancedOffset);
                k1 *= c1;
                k1 = rotl64(k1, 31);
                k1 *= c2;
                h1 ^= k1;
            default:
                break;
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
        int i16 = index << 4; //blocks are 16 bytes
        int blockOffset = offset + i16;

        ByteBuffer buffer = key.asByteBuffer().order(ByteOrder.nativeOrder());
        buffer.position(blockOffset);
        return buffer.getLong();
    }

    private static long rotl64(long v, int n) {
        return ((v << n) | (v >>> (64 - n)));
    }

    private static long fmix(long k) {
        long result = k;

        result ^= result >>> 33;
        result *= 0xff51afd7ed558ccdL;
        result ^= result >>> 33;
        result *= 0xc4ceb9fe1a85ec53L;
        result ^= result >>> 33;

        return result;
    }

}
