package ru.vk.itmo.reshetnikovaleksei.iterator;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.Iterator;

import ru.vk.itmo.BaseEntry;
import ru.vk.itmo.Entry;

public class SSTableIterator implements Iterator<Entry<MemorySegment>> {
    private final long keyIndexTo;
    private final MemorySegment ssTable;

    private long currentIndex;

    public SSTableIterator(long keyIndexTo, MemorySegment ssTable, long currentIndex) {
        this.keyIndexTo = keyIndexTo;
        this.ssTable = ssTable;
        this.currentIndex = currentIndex;
    }

    @Override
    public boolean hasNext() {
        return currentIndex < keyIndexTo;
    }

    @Override
    public Entry<MemorySegment> next() {
        long indexOffset = Long.BYTES + currentIndex * Byte.SIZE;
        currentIndex++;

        long localOffset = ssTable.get(ValueLayout.JAVA_LONG_UNALIGNED, indexOffset);
        long keySize = ssTable.get(ValueLayout.JAVA_LONG_UNALIGNED, localOffset);
        localOffset += Long.BYTES;

        MemorySegment key = ssTable.asSlice(localOffset, keySize);
        localOffset += keySize;

        long valueSize = ssTable.get(ValueLayout.JAVA_LONG_UNALIGNED, localOffset);
        localOffset += Long.BYTES;

        if (valueSize == -1) {
            return new BaseEntry<>(key, null);
        }

        return new BaseEntry<>(key, ssTable.asSlice(localOffset, valueSize));
    }
}
