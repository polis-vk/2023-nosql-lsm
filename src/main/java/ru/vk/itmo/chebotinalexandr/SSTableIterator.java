package ru.vk.itmo.chebotinalexandr;

import ru.vk.itmo.BaseEntry;
import ru.vk.itmo.Entry;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.Iterator;

public class SSTableIterator implements Iterator<Entry<MemorySegment>> {
    private static final long TOMBSTONE = -1;
    private long index;
    private final long keyIndexTo;
    private final MemorySegment sstable;

    public SSTableIterator(MemorySegment sstable, long keyIndexFrom, long keyIndexTo) {
        this.sstable = sstable;
        this.index = keyIndexFrom;
        this.keyIndexTo = keyIndexTo;
    }

    @Override
    public boolean hasNext() {
        return index < keyIndexTo;
    }

    @Override
    public Entry<MemorySegment> next() {
        Entry<MemorySegment> entry = next(Long.BYTES + index * Byte.SIZE);
        index++;

        return entry;
    }

    private Entry<MemorySegment> next(long offset) {
        long keyOffset = sstable.get(ValueLayout.JAVA_LONG_UNALIGNED, offset);
        long keySize = sstable.get(ValueLayout.JAVA_LONG_UNALIGNED, keyOffset);
        keyOffset += Long.BYTES;
        MemorySegment key = sstable.asSlice(keyOffset, keySize);
        keyOffset += keySize;
        long valueSize = sstable.get(ValueLayout.JAVA_LONG_UNALIGNED, keyOffset);
        keyOffset += Long.BYTES;

        if (valueSize == TOMBSTONE) {
            return new BaseEntry<>(key, null);
        } else {
            return new BaseEntry<>(key, sstable.asSlice(keyOffset, valueSize));
        }
    }
}
