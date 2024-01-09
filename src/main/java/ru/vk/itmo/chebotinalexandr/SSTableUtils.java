package ru.vk.itmo.chebotinalexandr;

import ru.vk.itmo.Entry;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

import static ru.vk.itmo.chebotinalexandr.SSTablesStorage.HEADER_OFFSET;

public final class SSTableUtils {

    private SSTableUtils() {

    }

    public static long binarySearch(MemorySegment readSegment, MemorySegment key) {
        long low = -1;
        long high = readSegment.get(ValueLayout.JAVA_LONG_UNALIGNED, HEADER_OFFSET);

        while (low < high - 1) {
            long mid = (high - low) / 2 + low;

            long offset = readSegment.get(ValueLayout.JAVA_LONG_UNALIGNED, Long.BYTES + mid * Byte.SIZE);
            long keySize = readSegment.get(ValueLayout.JAVA_LONG_UNALIGNED, offset);
            offset += Long.BYTES;

            long mismatch = MemorySegment.mismatch(readSegment, offset, offset + keySize,
                    key, 0, key.byteSize());

            if (mismatch == -1) {
                return mid;
            }

            if (mismatch == keySize) {
                low = mid;
                continue;
            }
            if (mismatch == key.byteSize()) {
                high = mid;
                continue;
            }

            int compare = Byte.compare(readSegment.get(ValueLayout.JAVA_BYTE, offset + mismatch),
                    key.get(ValueLayout.JAVA_BYTE, mismatch));

            if (compare > 0) {
                high = mid;
            } else {
                low = mid;
            }
        }

        return low + 1;
    }

    public static long entryByteSize(Entry<MemorySegment> entry) {
        if (entry.value() == null) {
            return entry.key().byteSize();
        }

        return entry.key().byteSize() + entry.value().byteSize();
    }
}
