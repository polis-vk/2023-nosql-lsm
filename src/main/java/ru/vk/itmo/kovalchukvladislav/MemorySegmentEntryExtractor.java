package ru.vk.itmo.kovalchukvladislav;

import ru.vk.itmo.BaseEntry;
import ru.vk.itmo.Entry;
import ru.vk.itmo.kovalchukvladislav.model.EntryExtractor;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

public class MemorySegmentEntryExtractor implements EntryExtractor<MemorySegment, Entry<MemorySegment>> {
    public static final MemorySegmentEntryExtractor INSTANCE = new MemorySegmentEntryExtractor();
    private static final ValueLayout.OfLong LONG_LAYOUT = ValueLayout.JAVA_LONG_UNALIGNED;
    private static final ValueLayout.OfByte BYTE_LAYOUT = ValueLayout.JAVA_BYTE;
    private static final int SIZE_LENGTH = Long.BYTES;
    private static final long VALUE_IS_NULL_SIZE = -1;

    @Override
    public MemorySegment readValue(MemorySegment memorySegment, long offset) {
        long size = memorySegment.get(LONG_LAYOUT, offset);
        if (size == VALUE_IS_NULL_SIZE) {
            return null;
        }
        return memorySegment.asSlice(offset + SIZE_LENGTH, size);
    }

    @Override
    public long writeValue(MemorySegment valueSegment, MemorySegment memorySegment, long offset) {
        if (valueSegment == null) {
            memorySegment.set(LONG_LAYOUT, offset, VALUE_IS_NULL_SIZE);
            return offset + SIZE_LENGTH;
        }
        long size = valueSegment.byteSize();
        memorySegment.set(LONG_LAYOUT, offset, size);
        MemorySegment.copy(valueSegment, 0, memorySegment, offset + SIZE_LENGTH, size);
        return offset + SIZE_LENGTH + size;
    }

    @Override
    public Entry<MemorySegment> readEntry(MemorySegment memorySegment, long offset) {
        MemorySegment key = readValue(memorySegment, offset);
        long valueOffset = offset + size(key);
        MemorySegment value = readValue(memorySegment, valueOffset);
        return new BaseEntry<>(key, value);
    }

    @Override
    public long writeEntry(Entry<MemorySegment> entry, MemorySegment memorySegment, long offset) {
        long valueOffset = writeValue(entry.key(), memorySegment, offset);
        return writeValue(entry.value(), memorySegment, valueOffset);
    }

    @Override
    public long findLowerBoundValueOffset(MemorySegment key, MemorySegment storage, MemorySegment offsets) {
        long entriesCount = offsets.byteSize() / SIZE_LENGTH;
        long left = -1;
        long right = entriesCount;

        while (left + 1 < right) {
            long middle = left + (right - left) / 2;
            long middleOffset = offsets.getAtIndex(LONG_LAYOUT, middle);
            MemorySegment middleKey = readValue(storage, middleOffset);

            if (compare(middleKey, key) <= 0) {
                left = middle;
            } else {
                right = middle;
            }
        }
        return left == -1 ? -1 : offsets.getAtIndex(LONG_LAYOUT, left);
    }

    @Override
    public long size(MemorySegment value) {
        if (value == null) {
            return SIZE_LENGTH;
        }
        return SIZE_LENGTH + value.byteSize();
    }

    @Override
    public long size(Entry<MemorySegment> entry) {
        return size(entry.key()) + size(entry.value());
    }

    @Override
    public Entry<MemorySegment> createEntry(MemorySegment key, MemorySegment value) {
        return new BaseEntry<>(key, value);
    }

    @Override
    public int compare(MemorySegment a, MemorySegment b) {
        long diffIndex = a.mismatch(b);
        if (diffIndex == -1) {
            return 0;
        } else if (diffIndex == a.byteSize()) {
            return -1;
        } else if (diffIndex == b.byteSize()) {
            return 1;
        }

        byte byteA = a.getAtIndex(BYTE_LAYOUT, diffIndex);
        byte byteB = b.getAtIndex(BYTE_LAYOUT, diffIndex);
        return Byte.compare(byteA, byteB);
    }
}
