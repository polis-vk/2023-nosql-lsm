package ru.vk.itmo.dyagayalexandra;

import ru.vk.itmo.Entry;

import java.lang.foreign.MemorySegment;
import java.util.Iterator;
import java.util.NoSuchElementException;

public class SkipNullIterator implements Iterator<Entry<MemorySegment>> {

    private final PeekingIterator iterator;

    public SkipNullIterator(PeekingIterator iterator) {
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
        if (!hasNext()) {
            throw new NoSuchElementException("There is no next element.");
        }

        return iterator.next();
    }
}
