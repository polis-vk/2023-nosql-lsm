package ru.vk.itmo.tuzikovalexandr;

import ru.vk.itmo.Entry;

import java.lang.foreign.MemorySegment;
import java.util.*;

public class RangeIterator implements Iterator<Entry<MemorySegment>> {

    private final Queue<PeekIterator> iterators = new PriorityQueue<>(
            MemorySegmentComparator::iteratorsCompare
    );
    private MemorySegment previousSegment;

    public RangeIterator(List<PeekIterator> iterators) {
        iterators.removeIf(i -> !i.hasNext());
        this.iterators.addAll(iterators);
    }

    @Override
    public boolean hasNext() {
        PeekIterator nextPeekIterator;
        while ((nextPeekIterator = iterators.peek()) != null) {
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
        Entry<MemorySegment> result = reInsert();
        previousSegment = result.key();
        return result;
    }

    private Entry<MemorySegment> reInsert() {
        PeekIterator nextElem = iterators.poll();
        if (nextElem == null) {
            throw new NoSuchElementException();
        }

        Entry<MemorySegment> res = nextElem.next();
        if (nextElem.hasNext()) {
            iterators.add(nextElem);
        }
        return res;
    }

    private boolean isEquals(MemorySegment o1, MemorySegment o2) {
        if (o2 == null) {
            return false;
        }
        return MemorySegmentComparator.compare(o1, o2) == 0;
    }
}
