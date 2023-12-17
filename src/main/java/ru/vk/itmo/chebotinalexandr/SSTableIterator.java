package ru.vk.itmo.chebotinalexandr;

import ru.vk.itmo.BaseEntry;
import ru.vk.itmo.Entry;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.Iterator;

import static ru.vk.itmo.chebotinalexandr.SSTableUtils.TOMBSTONE;

public class SSTableIterator implements Iterator<Entry<MemorySegment>> {
    private long index;
    private final long keyIndexTo;
    private final long keyOffset;
    private final MemorySegment sstable;

    public SSTableIterator(MemorySegment sstable, long keyIndexFrom, long keyIndexTo, long keyOffset) {
        this.sstable = sstable;
        this.index = keyIndexFrom;
        this.keyIndexTo = keyIndexTo;
        this.keyOffset = keyOffset;
    }

    @Override
    public boolean hasNext() {
        return index < keyIndexTo;
    }

    @Override
    public Entry<MemorySegment> next() {
        Entry<MemorySegment> entry = next(keyOffset + index * Byte.SIZE);
        index++;

        return entry;
    }

    private Entry<MemorySegment> next(long offset) {
        long keysOffset = sstable.get(ValueLayout.JAVA_LONG_UNALIGNED, offset);
        long keySize = sstable.get(ValueLayout.JAVA_LONG_UNALIGNED, keysOffset);
        keysOffset += Long.BYTES;
        MemorySegment key = sstable.asSlice(keysOffset, keySize);
        keysOffset += keySize;
        long valueSize = sstable.get(ValueLayout.JAVA_LONG_UNALIGNED, keysOffset);
        keysOffset += Long.BYTES;

        if (valueSize == TOMBSTONE) {
            return new BaseEntry<>(key, null);
        } else {
            return new BaseEntry<>(key, sstable.asSlice(keysOffset, valueSize));
        }
    }
}
