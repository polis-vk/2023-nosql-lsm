package ru.vk.itmo.test.emelyanovvitaliy;

import ru.vk.itmo.BaseEntry;
import ru.vk.itmo.Entry;

import java.io.Console;
import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Comparator;

import static java.nio.channels.FileChannel.MapMode.READ_ONLY;

public class FileIterator {
    private final Path filePath;
    private final FileChannel fc;
    private final MemorySegment mapped;
    private final Arena arena;
    private final Comparator<MemorySegment> comparator;
    private long nowKeyAddressOffset = Integer.BYTES + Long.BYTES;
    private int numOfKeys = -1;
    public FileIterator(Path filePath, Comparator<MemorySegment> comparator) throws IOException {
        this.filePath = filePath;
        this.fc = FileChannel.open(filePath, StandardOpenOption.READ);
        this.arena = Arena.ofShared();
        this.mapped = fc.map(READ_ONLY, 0, fc.size(), arena);
        this.comparator = comparator;
    }

    public void positionate(MemorySegment key) {
        int numOfKeys = getNumOfKeys();
        long firstKeyOffset = getAddressOffset(numOfKeys);
        int left = 0;
        int right = numOfKeys;
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

    public boolean hasNext() {
        return nowKeyAddressOffset < getAddressOffset(getNumOfKeys());
    }

    public Path getFilePath() {
        return filePath;
    }

    public int getNumOfKeys() {
        if (numOfKeys == -1) {
            numOfKeys = mapped.get(ValueLayout.JAVA_INT_UNALIGNED, Long.BYTES);
        }
        return numOfKeys;
    }

    private static long getAddressOffset(int n) {
        return Integer.BYTES + (2L * n + 1) * Long.BYTES;
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

//    private long getNextKeyOffset(long keyAddressOffset) {
//        long nextKeyAddressOffset = getNextKeyAddressOffset(keyAddressOffset);
//        if (nextKeyAddressOffset >= mapped.byteSize()) {
//            return mapped.byteSize();
//        } else {
//            return mapped.get(ValueLayout.JAVA_LONG_UNALIGNED, nextKeyAddressOffset);
//        }
//    }

    private long getNextKeyOffset(long keyAddressOffset) {
        long nextKeyAddressOffset = keyAddressOffset + 2L * Long.BYTES;
        if (nextKeyAddressOffset >= mapped.byteSize()) {
            return mapped.byteSize();
        } else {
            return Math.min(
                    mapped.get(ValueLayout.JAVA_LONG_UNALIGNED, nextKeyAddressOffset),
                    mapped.byteSize()
            );
        }
    }

    public static void main(String[] args) {
        System.out.println("Hello, world!");
    }

}
