package ru.vk.itmo.proninvalentin.iterators;

import ru.vk.itmo.Entry;

import java.lang.foreign.MemorySegment;

// Данный итератор нужен только для того, чтобы итератор по записям в памяти пропускал null записи
public class SkipDeletedEntryIterator implements PeekingPriorityIterator {
    private final PeekingPriorityIteratorImpl iterator;

    public SkipDeletedEntryIterator(PeekingPriorityIteratorImpl iterator) {
        this.iterator = iterator;
    }

    @Override
    public boolean hasNext() {
        if (iterator.hasNext() && iterator.getCurrent() == null) {
            iterator.next();
        }
        while (iterator.hasNext() && iterator.getCurrent().value() == null) {
            iterator.next();
        }
        return iterator.hasNext();
    }

    @Override
    public Entry<MemorySegment> next() {
        return iterator.next();
    }

    @Override
    public int compareTo(PeekingPriorityIterator another) {
        return iterator.compareTo(another);
    }

    @Override
    public Entry<MemorySegment> getCurrent() {
        return iterator.getCurrent();
    }

    @Override
    public int getPriority() {
        return iterator.getPriority();
    }
}
