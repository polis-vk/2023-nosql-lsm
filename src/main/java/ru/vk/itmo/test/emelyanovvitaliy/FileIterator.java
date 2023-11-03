package ru.vk.itmo.test.emelyanovvitaliy;

import ru.vk.itmo.BaseEntry;
import ru.vk.itmo.Entry;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.Comparator;

public class FileIterator {
    private final MemorySegment mapped;
    private final Comparator<MemorySegment> comparator;
    private long nowKeyAddressOffset = Integer.BYTES + 2 * Long.BYTES;
    private int numOfKeys = -1;
    private long timestamp = -1;
    private long runtimeTimestamp = -1;

    public FileIterator(MemorySegment memorySegment, Comparator<MemorySegment> comparator) {
        mapped = memorySegment;
        this.comparator = comparator;
    }

    public void positionate(MemorySegment key) {
        long firstKeyOffset = getAddressOffset(getNumOfKeys());
        int left = 0;
        int right = getNumOfKeys();
        while (left < right) {
            int m = (left + right) / 2;
            long keyAddressOffset = getAddressOffset(m);
            long keyOffset = mapped.get(ValueLayout.JAVA_LONG_UNALIGNED, keyAddressOffset);
            long valueOffset = mapped.get(ValueLayout.JAVA_LONG_UNALIGNED, keyAddressOffset + Long.BYTES);
            long keyOffsetTo;
            if (valueOffset == -1) {
                keyOffsetTo = getNextKeyOffset(keyAddressOffset);
            } else {
                keyOffsetTo = valueOffset;
            }
            long compared = comparator.compare(
                    key,
                    mapped.asSlice(keyOffset, keyOffsetTo - keyOffset)
            );

            if (compared == 0) {
                nowKeyAddressOffset = keyAddressOffset;
                return;
            } else if (compared > 0) {
                left = m + 1;
            } else {
                right = m;
            }
        }
        nowKeyAddressOffset = Math.min(getAddressOffset(left), firstKeyOffset);
    }

    public Entry<MemorySegment> getNow() {
        return getEntryByAddressOffset(nowKeyAddressOffset);
    }

    public void next() {
        nowKeyAddressOffset += 2L * Long.BYTES;
    }

    public boolean hasNow() {
        return nowKeyAddressOffset < getAddressOffset(getNumOfKeys());
    }

    public int getNumOfKeys() {
        if (numOfKeys == -1) {
            numOfKeys = mapped.get(ValueLayout.JAVA_INT_UNALIGNED, 2 * Long.BYTES);
        }
        return numOfKeys;
    }

    public long getTimestamp() {
        if (timestamp == -1) {
            timestamp = mapped.get(ValueLayout.JAVA_LONG_UNALIGNED, 0);
        }
        return timestamp;
    }

    public long getRuntimeTimestamp() {
        if (runtimeTimestamp == -1) {
            runtimeTimestamp = mapped.get(ValueLayout.JAVA_LONG_UNALIGNED, Long.BYTES);
        }
        return runtimeTimestamp;
    }

    private static long getAddressOffset(int n) {
        return Integer.BYTES + (2L * n + 2) * Long.BYTES;
    }

    private Entry<MemorySegment> getEntryByAddressOffset(long keyAddressOffset) {
        long keyOffset = mapped.get(ValueLayout.JAVA_LONG_UNALIGNED, keyAddressOffset);
        long keyOffsetTo;
        long valueOffset = mapped.get(ValueLayout.JAVA_LONG_UNALIGNED, keyAddressOffset + Long.BYTES);
        long nextKeyOffset = getNextKeyOffset(keyAddressOffset);
        if (valueOffset == -1) {
            keyOffsetTo = nextKeyOffset;
        } else {
            keyOffsetTo = valueOffset;
        }
        return new BaseEntry<>(
                mapped.asSlice(keyOffset, keyOffsetTo - keyOffset),
                valueOffset == -1 ? null :
                        mapped.asSlice(valueOffset, nextKeyOffset - valueOffset)
        );
    }

    private long getNextKeyOffset(long keyAddressOffset) {
        long nextKeyAddressOffset = keyAddressOffset + 2L * Long.BYTES;
        if (nextKeyAddressOffset >= getAddressOffset(getNumOfKeys())) {
            return mapped.byteSize();
        } else {
            return Math.min(
                    mapped.get(ValueLayout.JAVA_LONG_UNALIGNED, nextKeyAddressOffset),
                    mapped.byteSize()
            );
        }
    }
}
