package ru.vk.itmo.kobyzhevaleksandr;

import ru.vk.itmo.Entry;

import java.lang.foreign.MemorySegment;
import java.util.Comparator;

public class PriorityPeekIteratorComparator implements Comparator<PriorityPeekIterator<Entry<MemorySegment>>> {

    private final MemorySegmentComparator memorySegmentComparator;

    public PriorityPeekIteratorComparator(MemorySegmentComparator memorySegmentComparator) {
        this.memorySegmentComparator = memorySegmentComparator;
    }

    @Override
    public int compare(PriorityPeekIterator<Entry<MemorySegment>> priorityPeekIterator1,
                       PriorityPeekIterator<Entry<MemorySegment>> priorityPeekIterator2) {
        if (!priorityPeekIterator1.hasNext()) {
            return 1;
        }
        if (!priorityPeekIterator2.hasNext()) {
            return -1;
        }

        Entry<MemorySegment> entry1 = priorityPeekIterator1.peek();
        Entry<MemorySegment> entry2 = priorityPeekIterator2.peek();

        int result = memorySegmentComparator.compare(entry1.key(), entry2.key());
        if (result == 0) {
            return priorityPeekIterator1.priority() - priorityPeekIterator2.priority();
        }
        return result;
    }
}
