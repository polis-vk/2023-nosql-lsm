package ru.vk.itmo.test.ryabovvadim;

import java.util.ArrayList;
import java.util.List;

public class NumberUtils {

    public static long fromBytes(byte[] bytes) {
        if (bytes.length > 8) {
            throw new IllegalArgumentException("Bytes arrays is too big for long [size: " + bytes.length + "]");
        }

        long result = 0L;
        for (byte value : bytes) {
            result = (result << 8) + value;
        }

        return result;
    }

    public static byte[] toBytes(long value) {
        List<Byte> bytes = new ArrayList<>();

        while (value > 0) {
            bytes.add((byte) (value & 0xff));
            value >>= 8;
        }

        byte[] result = new byte[bytes.size()];
        for (int i = 0; i < bytes.size(); ++i) {
            result[i] = bytes.get(bytes.size() - i - 1);
        }

        return result;
    }
    
    private NumberUtils() {}
}
