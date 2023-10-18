package ru.vk.itmo.test.kononovvladimir;

import ru.vk.itmo.BaseEntry;
import ru.vk.itmo.Entry;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.Comparator;
import java.util.Iterator;

public class InFileIterator implements Iterator<Entry<MemorySegment>> {
    MemorySegment indexSegment;
    MemorySegment dataSegment;
    MemorySegment keySegment;
   Entry<MemorySegment>lastInMemoryEntry;

    public InFileIterator(MemorySegment indexSegment, MemorySegment dataSegment, MemorySegment keySegment, long current, long to, Iterator<Entry<MemorySegment>> inMemoryIterator, Comparator<MemorySegment> memorySegmentComparator) {
        this.indexSegment = indexSegment;
        this.dataSegment = dataSegment;
        this.keySegment = keySegment;
        this.current = current;
        this.to = to;
        this.inMemoryIterator = inMemoryIterator;
        this.memorySegmentComparator = memorySegmentComparator;
        if (inMemoryIterator.hasNext()) {
            lastInMemoryEntry = inMemoryIterator.next();
        } else {
            lastInMemoryEntry = null;
        }
    }

    long current;
    long to;
    Iterator<Entry<MemorySegment>> inMemoryIterator;
    Comparator<MemorySegment> memorySegmentComparator;

    @Override
    public boolean hasNext() {
        return inMemoryIterator.hasNext() || current <= to;
    }

    @Override
    public Entry<MemorySegment> next(){
        if (!hasNext()) return null;
        if (current > to) {
            nextInMemoryEntry();
            return lastInMemoryEntry;
        }

        long dataOffset = indexSegment.get(ValueLayout.JAVA_LONG_UNALIGNED, Long.BYTES * 3L * current + Long.BYTES + Long.BYTES);
        long keyOffset = indexSegment.get(ValueLayout.JAVA_LONG_UNALIGNED, Long.BYTES * 3L * current + Long.BYTES + Long.BYTES * 2);

        long dataSize = dataSegment.get(ValueLayout.JAVA_LONG_UNALIGNED, dataOffset);
        long keySize = keySegment.get(ValueLayout.JAVA_LONG_UNALIGNED, keyOffset);
        dataOffset += Long.BYTES;
        keyOffset += Long.BYTES;
        MemorySegment memorySegmentInFileData = dataSegment.asSlice(dataOffset, dataSize);
        MemorySegment memorySegmentInFileKey = keySegment.asSlice(keyOffset, keySize);

        if (lastInMemoryEntry == null){
            current++;
            return new BaseEntry<>(memorySegmentInFileKey, memorySegmentInFileData);
        }

        MemorySegment memorySegmentInMemoryKey = lastInMemoryEntry.key();
        long compare = memorySegmentComparator.compare(memorySegmentInMemoryKey, memorySegmentInFileKey);
        Entry<MemorySegment> result;
        if (compare <= 0){
            if (compare == 0) {
                current++;
            }
            result = lastInMemoryEntry;
            nextInMemoryEntry();
        } else {
            result = new BaseEntry<>(memorySegmentInFileKey, memorySegmentInFileData);
            current++;
        }
        return result;
    }

    private void nextInMemoryEntry(){
        if (inMemoryIterator.hasNext()) {
            lastInMemoryEntry = inMemoryIterator.next();
        } else {
            lastInMemoryEntry = null;
        }
    }
}
