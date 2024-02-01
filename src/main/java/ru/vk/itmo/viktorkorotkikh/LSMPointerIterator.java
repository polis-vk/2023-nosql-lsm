package ru.vk.itmo.viktorkorotkikh;

import ru.vk.itmo.Entry;

import java.lang.foreign.MemorySegment;
import java.util.Iterator;

public abstract class LSMPointerIterator implements Iterator<Entry<MemorySegment>> {

    abstract int getPriority();

    protected abstract MemorySegment getPointerKeySrc();

    protected abstract long getPointerKeySrcOffset();

    protected abstract long getPointerKeySrcSize();

    abstract boolean isPointerOnTombstone();

    abstract void shift();

    abstract long getPointerSize();

    public int compareByPointers(LSMPointerIterator otherIterator) {
        return MemorySegmentComparator.INSTANCE.compare(
                getPointerKeySrc(),
                getPointerKeySrcOffset(),
                getPointerKeySrcOffset() + getPointerKeySrcSize(),
                otherIterator.getPointerKeySrc(),
                otherIterator.getPointerKeySrcOffset(),
                otherIterator.getPointerKeySrcOffset() + otherIterator.getPointerKeySrcSize()
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
