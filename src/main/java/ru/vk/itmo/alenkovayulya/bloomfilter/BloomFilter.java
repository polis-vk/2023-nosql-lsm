package ru.vk.itmo.alenkovayulya.bloomfilter;

import java.lang.foreign.MemorySegment;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class BloomFilter {
    private final FilterStorage filter;
    private final int hashCount;
    private int bitsSize;

    public static BloomFilter createBloom(int n) {
        return new BloomFilter(n, 0.001);
    }

    private BloomFilter(int n, double p) {
        bitsSize = (int) ((-n * Math.log(p)) / Math.pow(Math.log(2), 2));
        int bytesSize = (bitsSize + 7) / 8;
        int capacity = bytesSize / 8 + 1;
        bitsSize = capacity * 64;
        filter = new FilterStorage(capacity);

        hashCount = Utils.countOptimalHashFunctions(bitsSize, n);
    }


    public static void fillIndexes(long base, long inc, int hashCount, int bitsetSize, long[] result) {
        for (int i = 0; i < hashCount; i++) {
            result[i] = (int) Utils.abs(base % bitsetSize);
            base += inc;
        }
    }

    public void add(MemorySegment key) {
        long[] indexes = new long[hashCount];
        Utils.hashKey(key, indexes);

        fillIndexes(indexes[1], indexes[0], hashCount, bitsSize, indexes);
        setIndexes(indexes);
    }

    private void setIndexes(long[] indexes) {
        for (long index : indexes) {
            filter.set((int) index);
        }
    }

    public FilterStorage getFilter() {
        return filter;
    }

    public int getFilterSize() {
        return filter.size();
    }

    public static class FilterStorage {

        private final List<Long> longs;

        public FilterStorage(int capacity) {
            this.longs = new ArrayList<>(Collections.nCopies(capacity, 0L));
        }


        public boolean get(int i) {
            int arrayOffset = i >> 6;
            int longOffset = i - (arrayOffset << 6);

            return ((1L << (63 - longOffset)) & longs.get(arrayOffset)) != 0;
        }

        public void set(int i) {
            int arrayOffset = i >> 6;
            int longOffset = i - (arrayOffset << 6);

            longs.set(arrayOffset, longs.get(arrayOffset) | (1L << (63 - longOffset)));
        }

        public List<Long> getLongs() {
            return Collections.unmodifiableList(longs);
        }

        public int size() {
            return longs.size();
        }
    }

}
