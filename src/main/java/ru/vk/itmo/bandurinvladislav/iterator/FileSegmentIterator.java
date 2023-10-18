package ru.vk.itmo.bandurinvladislav.iterator;

import ru.vk.itmo.BaseEntry;
import ru.vk.itmo.Entry;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

import static ru.vk.itmo.bandurinvladislav.util.Constants.*;

public class FileSegmentIterator implements MemorySegmentIterator {

    private final MemorySegment indexSegment;
    private final MemorySegment sstableSegment;
    private final long sstableUpperBound;
    private final int priority;
    private Entry<MemorySegment> minKeyEntry;
    private long sstableOffset;
    private long indexOffset;

    public FileSegmentIterator(MemorySegment sstableSegment, MemorySegment indexSegment, long sstableOffset,
                               long indexOffset, long sstableUpperBound, int priority) {
        this.indexSegment = indexSegment;
        this.sstableSegment = sstableSegment;
        this.priority = priority;
        this.sstableOffset = sstableOffset;
        this.indexOffset = indexOffset;
        this.sstableUpperBound = sstableUpperBound;
        this.minKeyEntry = getNext();
    }

    @Override
    public boolean hasNext() {
        return sstableOffset <= sstableUpperBound;
    }

    @Override
    public Entry<MemorySegment> next() {
        var result = minKeyEntry;
        minKeyEntry = getNext();
        return result;
    }

    @Override
    public Entry<MemorySegment> peek() {
        return minKeyEntry;
    }

    @Override
    public int getPriority() {
        return priority;
    }

    private Entry<MemorySegment> getNext() {
        if (hasNext()) {
            long keySize = indexSegment.get(ValueLayout.JAVA_LONG_UNALIGNED,
                    indexOffset + INDEX_ROW_KEY_LENGTH_POSITION);
            long valueSize = indexSegment.get(ValueLayout.JAVA_LONG_UNALIGNED,
                    indexOffset + INDEX_ROW_VALUE_LENGTH_POSITION);

            var result = new BaseEntry<>(
                    sstableSegment.asSlice(sstableOffset, keySize),
                    sstableSegment.asSlice(sstableOffset + keySize, valueSize)
            );
            sstableOffset += keySize + valueSize;
            indexOffset += INDEX_ROW_SIZE;
            return result;
        }
        return null;
    }
}
