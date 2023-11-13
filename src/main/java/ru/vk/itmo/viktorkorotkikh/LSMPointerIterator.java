package ru.vk.itmo.viktorkorotkikh;

import ru.vk.itmo.Entry;

import java.lang.foreign.MemorySegment;
import java.util.Iterator;

abstract class LSMPointerIterator implements Iterator<Entry<MemorySegment>> {

    abstract int getPriority();

    protected abstract MemorySegment getPointerSrc();

    protected abstract long getPointerSrcOffset();

    protected abstract long getPointerSrcSize();

    abstract boolean isPointerOnTombstone();

    public int compareByPointers(LSMPointerIterator otherIterator) {
        return MemorySegmentComparator.INSTANCE.compare(
                getPointerSrc(),
                getPointerSrcOffset(),
                getPointerSrcOffset() + getPointerSrcSize(),
                otherIterator.getPointerSrc(),
                otherIterator.getPointerSrcOffset(),
                otherIterator.getPointerSrcOffset() + otherIterator.getPointerSrcSize()
        );
    }

    public int compareByPointersWithPriority(LSMPointerIterator otherIterator) {
        int keyComparison = compareByPointers(otherIterator);
        if (keyComparison == 0) {
            return Integer.compare(otherIterator.getPriority(), getPriority());
        }
        return keyComparison;
    }
}
