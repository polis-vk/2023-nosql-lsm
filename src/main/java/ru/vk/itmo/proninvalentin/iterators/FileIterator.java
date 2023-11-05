package ru.vk.itmo.proninvalentin.iterators;

import ru.vk.itmo.Entry;
import ru.vk.itmo.proninvalentin.comparators.MemorySegmentComparator;
import ru.vk.itmo.proninvalentin.utils.MemorySegmentUtils;

import java.lang.foreign.MemorySegment;
import java.util.Comparator;
import java.util.Iterator;

public final class FileIterator implements Iterator<Entry<MemorySegment>> {
    private final MemorySegment readValuesMS;
    private final MemorySegment readOffsetsMS;
    private long curIndex;
    private long endIndex = -1;

    public FileIterator(MemorySegment readValuesMS,
                        MemorySegment readOffsetsMS,
                        MemorySegment from,
                        MemorySegment to) {
        this.readValuesMS = readValuesMS;
        this.readOffsetsMS = readOffsetsMS;
        Comparator<MemorySegment> comparator = MemorySegmentComparator.getInstance();

        if (from != null) {
            curIndex = MemorySegmentUtils.leftBinarySearch(readValuesMS, readOffsetsMS, from, comparator);
            if (curIndex == -1) {
                return;
            }
        }

        if (to != null) {
            endIndex = MemorySegmentUtils.leftBinarySearch(readValuesMS, readOffsetsMS, to, comparator);
        }

        if (endIndex == -1) {
            endIndex = readOffsetsMS.byteSize() / Long.BYTES;
        }
    }

    @Override
    public boolean hasNext() {
        return curIndex < endIndex;
    }

    @Override
    public Entry<MemorySegment> next() {
        return MemorySegmentUtils.getEntryByIndex(readValuesMS, readOffsetsMS, curIndex++);
    }
}
