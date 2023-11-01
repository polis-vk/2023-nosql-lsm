package ru.vk.itmo.test.kononovvladimir;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

public class FileSearcher {
    MemorySegment memorySegment;
    long currentIndex;
    long currentOffset;
    long size;

    public boolean canContinue(){
        return currentIndex < size;
    }

    public FileSearcher(MemorySegment memorySegment, long size) {
        this.memorySegment = memorySegment;
        this.currentIndex = 0;
        this.currentOffset = 0;
        this.size = size;
    }

    public long getLongAtOffsetAndGo() {
        long res = memorySegment.get(ValueLayout.JAVA_LONG_UNALIGNED, currentOffset);
        currentOffset += Long.BYTES;
        return res;
    }

    public MemorySegment getMemorySegmentAndGo(long size){
        if (size == -1) return null;
        MemorySegment memorySegmentInFile = memorySegment.asSlice(currentOffset, size);
        currentOffset += size;
        return memorySegmentInFile;
    }

    public MemorySegment getValueInStrokeAndGo() {
        MemorySegment res = getMemorySegmentAndGo(getLongAtOffsetAndGo());
        currentIndex++;
        return res;
    }

    public long getLongAtOffsetAnd(long offset){
        return memorySegment.get(ValueLayout.JAVA_LONG_UNALIGNED, offset);
    }

    public void goToOffset(long offset, long index) {
        this.currentIndex = index;
        this.currentOffset = offset;
    }

}
