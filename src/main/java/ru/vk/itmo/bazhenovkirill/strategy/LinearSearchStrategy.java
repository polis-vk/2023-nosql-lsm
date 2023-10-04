package ru.vk.itmo.bazhenovkirill.strategy;

import ru.vk.itmo.BaseEntry;
import ru.vk.itmo.Entry;
import ru.vk.itmo.bazhenovkirill.MemorySegmentComparator;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

public class LinearSearchStrategy implements ElementSearchStrategy {

    private final MemorySegmentComparator comparator = new MemorySegmentComparator();

    @Override
    public Entry<MemorySegment> search(MemorySegment data, MemorySegment key, long fileSize) {
        long offset = 0;
        while (offset < fileSize) {
            long keySize = data.get(ValueLayout.JAVA_LONG_UNALIGNED, offset);
            offset += Long.BYTES;

            MemorySegment possibleKey = data.asSlice(offset, keySize);
            offset += keySize;

            long valueSize = data.get(ValueLayout.JAVA_LONG_UNALIGNED, offset);
            offset += Long.BYTES;
            if (comparator.compare(possibleKey, key) == 0) {
                MemorySegment value = data.asSlice(offset, valueSize);
                return new BaseEntry<>(possibleKey, value);
            }
            offset += valueSize;
        }
        return null;
    }
}
