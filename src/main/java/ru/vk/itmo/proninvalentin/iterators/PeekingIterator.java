package ru.vk.itmo.proninvalentin.iterators;

import ru.vk.itmo.Entry;
import ru.vk.itmo.proninvalentin.comparators.MemorySegmentComparator;

import java.lang.foreign.MemorySegment;
import java.util.Iterator;
import java.util.NoSuchElementException;

public class PeekingIterator implements Iterator<Entry<MemorySegment>>, Comparable<PeekingIterator> {
    private final Iterator<Entry<MemorySegment>> iterator;
    private Entry<MemorySegment> current;
    private final int index;

    public PeekingIterator(Iterator<Entry<MemorySegment>> iterator, int index) {
        this.iterator = iterator;
        this.index = index;
    }

    public int getIndex() {
        return index;
    }

    @Override
    public boolean hasNext() {
        return iterator.hasNext() || current != null;
    }

    @Override
    public Entry<MemorySegment> next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        Entry<MemorySegment> res = peek();
        current = null;
        return res;
    }

    public Entry<MemorySegment> peek() {
        if (current == null) {
            if (!iterator.hasNext()) {
                throw new NoSuchElementException();
            }
            current = iterator.next();
            return current;
        }
        return current;
    }

    @Override
    public int compareTo(PeekingIterator another) {
        var compareResult = MemorySegmentComparator.getInstance().compare(this.current.key(), another.current.key());
        if (compareResult == 0) {
            return Integer.compare(this.index, another.index);
        } else {
            return compareResult;
        }
    }
}
