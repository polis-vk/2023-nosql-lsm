package ru.vk.itmo.tuzikovalexandr;

import ru.vk.itmo.Entry;

import java.lang.foreign.MemorySegment;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.PriorityQueue;

public class RangeIterator implements Iterator<Entry<MemorySegment>> {

    private final PriorityQueue<PeekIterator> iterators = new PriorityQueue<>(
            MemorySegmentComparator::iteratorsCompare
    );
    private MemorySegment previous;

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
                previous = current.key();
                reInsert();
                continue;
            }

            if (!isEquals(current.key(), previous)) {
                return true;
            }
            reInsert();
        }
        return false;
    }

    @Override
    public Entry<MemorySegment> next() {
        Entry<MemorySegment> result = reInsert();
        previous = result.key();
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
