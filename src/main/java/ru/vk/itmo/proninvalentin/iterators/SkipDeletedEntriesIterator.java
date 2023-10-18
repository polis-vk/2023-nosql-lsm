package ru.vk.itmo.proninvalentin.iterators;

import ru.vk.itmo.Entry;

import java.lang.foreign.MemorySegment;
import java.util.Iterator;

public class SkipDeletedEntriesIterator implements Iterator<Entry<MemorySegment>> {
    private final PeekingIterator iterator;

    public SkipDeletedEntriesIterator(PeekingIterator iterator) {
        this.iterator = iterator;
    }

    @Override
    public boolean hasNext() {
        while (iterator.hasNext() && iterator.peek().value() == null) {
            iterator.next();
        }
        return iterator.hasNext();
    }

    @Override
    public Entry<MemorySegment> next() {
        return iterator.next();
    }
}
