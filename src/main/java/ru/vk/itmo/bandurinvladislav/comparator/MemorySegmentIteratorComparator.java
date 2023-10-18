package ru.vk.itmo.bandurinvladislav.comparator;

import ru.vk.itmo.bandurinvladislav.iterator.MemorySegmentIterator;

import java.util.Comparator;

public class MemorySegmentIteratorComparator implements Comparator<MemorySegmentIterator> {
    private final MemorySegmentComparator memorySegmentComparator = new MemorySegmentComparator();

    @Override
    public int compare(MemorySegmentIterator i1, MemorySegmentIterator i2) {
        int compareResult = memorySegmentComparator.compare(i1.peek().key(), i2.peek().key());
        if (compareResult != 0) {
            return compareResult;
        }
        return i1.getPriority() - i2.getPriority();
    }
}
