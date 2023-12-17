package ru.vk.itmo.chebotinalexandr;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

import static ru.vk.itmo.chebotinalexandr.SSTableUtils.BLOOM_FILTER_BIT_SIZE_OFFSET;
import static ru.vk.itmo.chebotinalexandr.SSTableUtils.BLOOM_FILTER_HASH_FUNCTIONS_OFFSET;

public final class BloomFilter {
    private static final int LONG_ADDRESSABLE_BITS = 6;

    private BloomFilter() {

    }

    public static long divide(long p, long q) {
        long div = p / q;
        long rem = p - q * div;

        if (rem == 0) {
            return div;
        }

        int sgn = 1 | (int) ((p ^ q) >> (Long.SIZE - 1));
        boolean increment = sgn > 0;

        return increment ? div + sgn : div;
    }


    public static void addToSstable(MemorySegment key, MemorySegment sstable, int hashFunctionsNum, long bitSize) {
        long[] indexes = MurmurHash.hash64(key, 0, (int) key.byteSize());

        long base = indexes[0];
        long inc = indexes[1];

        long combinedHash = base;
        for (int i = 0; i < hashFunctionsNum; i++) {
            long bitIndex = (combinedHash & Long.MAX_VALUE) % bitSize;
            set(bitIndex, sstable);
            combinedHash += inc;
        }
    }

    private static void set(long bitIndex, MemorySegment sstable) {
        int longIndex = (int) (bitIndex >>> LONG_ADDRESSABLE_BITS); //this is an arrayIndex
        long bitOffset = 4L * Long.BYTES + longIndex * Long.BYTES;

        long mask = 1L << bitIndex;

        long oldValue = sstable.get(ValueLayout.JAVA_LONG_UNALIGNED, bitOffset);
        sstable.set(ValueLayout.JAVA_LONG_UNALIGNED, bitOffset, oldValue | mask);
    }

    public static boolean sstableMayContain(MemorySegment key, MemorySegment sstable) {
        long[] indexes = MurmurHash.hash64(key, 0, (int) key.byteSize(), MurmurHash.DEFAULT_SEED);

        long base = indexes[0];
        long inc = indexes[1];

        long bitSize = sstable.get(ValueLayout.JAVA_LONG_UNALIGNED, BLOOM_FILTER_BIT_SIZE_OFFSET);
        long hashFunctions = sstable.get(ValueLayout.JAVA_LONG_UNALIGNED, BLOOM_FILTER_HASH_FUNCTIONS_OFFSET);

        long combinedHash = base;
        for (int i = 0; i < hashFunctions; i++) {
            if (!getFromSstable((combinedHash & Long.MAX_VALUE) % bitSize, sstable)) {
                return false;
            }
            combinedHash += inc;
        }

        return true;
    }

    private static boolean getFromSstable(long bitIndex, MemorySegment sstable) {
        int longIndex = (int) (bitIndex >>> LONG_ADDRESSABLE_BITS);
        long bitOffset = 4L * Long.BYTES + longIndex * Long.BYTES;

        long hashFromSstable = sstable.get(ValueLayout.JAVA_LONG_UNALIGNED, bitOffset);
        return (hashFromSstable & (1L << bitIndex)) != 0;
    }

}
