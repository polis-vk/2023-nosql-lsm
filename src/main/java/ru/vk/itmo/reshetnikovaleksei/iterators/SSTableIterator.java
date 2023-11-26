package ru.vk.itmo.reshetnikovaleksei.iterators;

import ru.vk.itmo.BaseEntry;
import ru.vk.itmo.Entry;
import ru.vk.itmo.reshetnikovaleksei.MemorySegmentComparator;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.Iterator;
import java.util.NoSuchElementException;

public class SSTableIterator implements Iterator<Entry<MemorySegment>> {

    private final MemorySegment dataSegment;
    private final MemorySegment indexSegment;
    private final MemorySegment to;

    private long indexOffset;
    private long currentKeyOffset;
    private long currentKeySize;

    public SSTableIterator(long indexOffset, MemorySegment to, MemorySegment dataSegment, MemorySegment indexSegment) {
        this.indexOffset = indexOffset;
        this.to = to;
        this.dataSegment = dataSegment;
        this.indexSegment = indexSegment;

        this.currentKeyOffset = -1;
        this.currentKeySize = -1;
    }

    @Override
    public boolean hasNext() {
        if (indexOffset == indexSegment.byteSize()) {
            return false;
        }

        if (to == null) {
            return true;
        }
        currentKeyOffset = indexSegment.get(ValueLayout.JAVA_LONG_UNALIGNED, indexOffset);
        currentKeySize = dataSegment.get(ValueLayout.JAVA_LONG_UNALIGNED, currentKeyOffset);
        long fromOffset = currentKeyOffset + Long.BYTES;

        return MemorySegmentComparator.getInstance()
                .compare(to, dataSegment, fromOffset, fromOffset + currentKeySize) > 0;
    }

    @Override
    public Entry<MemorySegment> next() {
        if (!hasNext()) {
            throw new NoSuchElementException("No next element");
        }

        long keyOffset;
        long keySize;
        if (currentKeyOffset == -1 || currentKeySize == -1) {
            keyOffset = indexSegment.get(ValueLayout.JAVA_LONG_UNALIGNED, indexOffset);
            keySize = dataSegment.get(ValueLayout.JAVA_LONG_UNALIGNED, keyOffset);
        } else {
            keyOffset = currentKeyOffset;
            keySize = currentKeySize;
        }
        indexOffset += Long.BYTES;
        keyOffset += Long.BYTES;
        MemorySegment key = dataSegment.asSlice(keyOffset, keySize);
        keyOffset += keySize;

        long valueSize = dataSegment.get(ValueLayout.JAVA_LONG_UNALIGNED, keyOffset);
        MemorySegment value;
        if (valueSize == -1) {
            value = null;
        } else {
            value = dataSegment.asSlice(keyOffset + Long.BYTES, valueSize);
        }

        return new BaseEntry<>(key, value);
    }
}
