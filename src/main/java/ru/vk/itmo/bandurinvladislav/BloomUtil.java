package ru.vk.itmo.bandurinvladislav;

import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;

public class BloomUtil {

    public static long abs(long index) {
        long negbit = index >> 63;
        return (index ^ negbit) - negbit;
    }

    public static int evalHashCount(int mBits, int n) {
        int k = (int) (Math.log(2) * mBits / n);

        if (k < 1) {
            return 1;
        }
        return Math.min(k, 30);
    }

    public static void hashKey(MemorySegment key, long[] results) {
        ByteBuffer data = key.asByteBuffer();
        key.asByteBuffer();
        MurmurHash3.hash128x64(data, 0, data.capacity(), 0, results);
    }
}
