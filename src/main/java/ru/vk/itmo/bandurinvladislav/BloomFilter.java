package ru.vk.itmo.bandurinvladislav;

import java.lang.foreign.MemorySegment;

public class BloomFilter {
    private static final double DEFAULT_FALSE_POSITIVE_RATE = 0.01;
    private final BitSet filter;
    private final int hashCount;
    private int bitsSize;

    private BloomFilter(int n, double p) {
        // Optimal m: https://andybui01.github.io/bloom-filter/
        bitsSize = (int) ((-n * Math.log(p)) / Math.pow(Math.log(2), 2));
        int bytesSize = (bitsSize + 7) / 8;
        int bitsetCapacity = bytesSize / 8 + 1;
        bitsSize = bitsetCapacity * 64;
        filter = new BitSet(bitsetCapacity);

        /*
        Since I'm going to use combinatorial generation of hashes
        as described in https://github.com/Claudenw/BloomFilters/wiki/Bloom-Filters, it's not supposed to be
        very expensive to evaluate hashes for big k. You can see similar technique in levelDB implementation
        of bloom filter: https://github.com/google/leveldb/blob/main/util/bloom.cc
         */
        hashCount = BloomUtil.evalHashCount(bitsSize, n);
    }

    public BloomFilter(BitSet filter, int n) {
        this.filter = filter;
        bitsSize = filter.getLongs().size() * 64;
        hashCount = BloomUtil.evalHashCount(bitsSize, n);
    }

    public static BloomFilter createBloom(int n) {
        return new BloomFilter(n, DEFAULT_FALSE_POSITIVE_RATE);
    }

    public static void indexes(long base, long inc, int hashCount, int bitsetSize, long[] result) {
        for (int i = 0; i < hashCount; i++) {
            result[i] = (int) BloomUtil.abs(base % bitsetSize);
            base += inc;
        }
    }

    public void add(MemorySegment key) {
        long[] indexes = new long[hashCount];
        BloomUtil.hashKey(key, indexes);

        indexes(indexes[1], indexes[0], hashCount, bitsSize, indexes);
        setIndexes(indexes);
    }

    private void setIndexes(long[] indexes) {
        for (long index : indexes) {
            filter.set((int) index);
        }
    }

    public BitSet getFilter() {
        return filter;
    }

    public int getFilterSize() {
        return filter.size();
    }
}
