package ru.vk.itmo.alenkovayulya.bloomfilter;

import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;

public class Utils {

    private Utils() {}

    public static long abs(long index) {
        long negbit = index >> 63;
        return (index ^ negbit) - negbit;
    }

    public static void hashKey(MemorySegment key, long[] results) {
        ByteBuffer data = key.asByteBuffer();
        key.asByteBuffer();

        HashFunction.hash(data, 0, data.capacity(), 0, results);
    }

    public static int countOptimalHashFunctions(int bitsSize, int n) {
        int k = (int) (Math.log(2) * bitsSize / n);

        if (k < 1) {
            return 1;
        }
        return Math.min(k, 15);
    }

}
