package ru.vk.itmo.tuzikovalexandr;

import ru.vk.itmo.Entry;

import java.lang.foreign.MemorySegment;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.PriorityQueue;
import java.util.Queue;

public class RangeIterator implements Iterator<Entry<MemorySegment>> {

    private final Queue<PeekIterator> mergeIterators;
    private MemorySegment previousSegment;

    public RangeIterator(List<PeekIterator> peekIterators) {
        mergeIterators = new PriorityQueue<>(MemorySegmentComparator::iteratorsCompare);

        peekIterators.removeIf(i -> !i.hasNext());
        this.mergeIterators.addAll(peekIterators);
    }

    @Override
    public boolean hasNext() {
        PeekIterator nextPeekIterator;

        while ((nextPeekIterator = mergeIterators.peek()) != null) {
            Entry<MemorySegment> current = nextPeekIterator.peek();

            if (current.value() == null) {
                previousSegment = current.key();
                reInsert();
                continue;
            }

            if (!isEquals(current.key(), previousSegment)) {
                return true;
            }
            reInsert();
        }
        return false;
    }

    @Override
    public Entry<MemorySegment> next() {
        Entry<MemorySegment> resultEntry = reInsert();
        previousSegment = resultEntry.key();

        return resultEntry;
    }

    private Entry<MemorySegment> reInsert() {
        PeekIterator nextIterator = mergeIterators.poll();
        if (nextIterator == null) {
            throw new NoSuchElementException();
        }

        Entry<MemorySegment> resultEntry = nextIterator.next();
        if (nextIterator.hasNext()) {
            mergeIterators.add(nextIterator);
        }

        return resultEntry;
    }

    private boolean isEquals(MemorySegment o1, MemorySegment o2) {
        if (o2 == null) {
            return false;
        }
        return MemorySegmentComparator.compare(o1, o2) == 0;
    }
}
