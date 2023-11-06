package ru.vk.itmo.kobyzhevaleksandr;

import ru.vk.itmo.Entry;

import java.lang.foreign.MemorySegment;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * An iterator that ignores {@code null} values.
 * If the next value is {@code null}, then it is skipped and the next value is taken.
 * This happens until the value returned by the iterator is not {@code null}.
 */
public class SkipNullIterator implements Iterator<Entry<MemorySegment>> {

    private final PeekIterator<Entry<MemorySegment>> iterator;

    public SkipNullIterator(PeekIterator<Entry<MemorySegment>> iterator) {
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
            throw new NoSuchElementException();
        }
        return iterator.next();
    }
}
