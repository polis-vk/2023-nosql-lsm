package ru.vk.itmo.tuzikovalexandr;

import ru.vk.itmo.Entry;

import java.lang.foreign.MemorySegment;
import java.util.Iterator;
import java.util.NoSuchElementException;

public class FileIterator implements Iterator<Entry<MemorySegment>> {

    private final MemorySegment offsetSegment;
    private final MemorySegment dataSegment;
    private final long end;
    private long currentOffset;

    public FileIterator(MemorySegment offsetSegment, MemorySegment dataSegment, long start, long end) {
        this.offsetSegment = offsetSegment;
        this.dataSegment = dataSegment;
        this.end = end;
        currentOffset = start;
    }

    @Override
    public boolean hasNext() {
        return currentOffset <= end;
    }

    @Override
    public Entry<MemorySegment> next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }

        Entry<MemorySegment> memorySegmentEntry = Utils.getEntryByKeyOffset(currentOffset, offsetSegment, dataSegment);

        currentOffset += Long.BYTES * 2;
        return memorySegmentEntry;
    }
}
