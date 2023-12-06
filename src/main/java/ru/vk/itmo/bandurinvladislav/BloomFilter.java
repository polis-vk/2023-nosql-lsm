package ru.vk.itmo.bandurinvladislav;

import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;


public class BloomFilter {
    private final static double DEFAULT_FALSE_POSITIVE_RATE = 0.01;
    private final BitSet filter;
    private final int mBytes;
    private int mBits;
    private int k;

    private BloomFilter(int n, double p) {
        mBits = (int) ((-n * Math.log(p)) / Math.pow(Math.log(2), 2)); // Optimal m: https://andybui01.github.io/bloom-filter/
        mBytes = (mBits + 7) / 8;
        mBits = mBytes * 8;
        filter = new BitSet(mBytes);
        k = (int) (Math.log(2) * mBits / n);

        if (k < 1) {
            k = 1;
        }

        /*
        Since I'm going to use combinatorial generation of hashes
        as described in https://github.com/Claudenw/BloomFilters/wiki/Bloom-Filters, it's not supposed to be
        very expensive to evaluate hashes for big k. You can see similar technique in levelDB implementation
        of bloom filter: https://github.com/google/leveldb/blob/main/util/bloom.cc
         */
        if (k > 30) {
            k = 30;
        }
    }

    public static BloomFilter createBloom(int n) {
        return new BloomFilter(n, DEFAULT_FALSE_POSITIVE_RATE);
    }

    public static BloomFilter createBloom(int n, double p) {
        return new BloomFilter(n, p);
    }

    public void add(MemorySegment key) {
        long[] indexes = new long[2];
        ByteBuffer data = key.asByteBuffer();
        key.asByteBuffer();
        MurmurHash3.hash128x64(data, 0, data.capacity(), MurmurHash3.DEFAULT_SEED, indexes);

        setIndexes(indexes[1], indexes[0], k, mBits, indexes);
    }

    public void setIndexes(long base, long inc, int hashCount, int bitsetSize, long[] result) {
        for (int i = 0; i < hashCount; i++) {
            filter.set((int) abs(base % bitsetSize));
            base += inc;
        }
    }

    public BitSet getFilter() {
        return filter;
    }

    public int getFilterSize() {
        return mBytes;
    }

    public static long abs(long index) {
        long negbit = index >> 63;
        return (index ^ negbit) - negbit;
    }
}
