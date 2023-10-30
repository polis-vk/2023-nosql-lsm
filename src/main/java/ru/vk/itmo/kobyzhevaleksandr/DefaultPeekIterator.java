package ru.vk.itmo.kobyzhevaleksandr;

import ru.vk.itmo.Entry;

import java.lang.foreign.MemorySegment;
import java.util.Iterator;
import java.util.NoSuchElementException;

public class DefaultPeekIterator implements PeekIterator<Entry<MemorySegment>> {

    private final Iterator<Entry<MemorySegment>> iterator;
    private Entry<MemorySegment> peekedEntry;

    public DefaultPeekIterator(Iterator<Entry<MemorySegment>> iterator) {
        this.iterator = iterator;
    }

    @Override
    public boolean hasNext() {
        return peekedEntry != null || iterator.hasNext();
    }

    @Override
    public Entry<MemorySegment> next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        if (peekedEntry == null) {
            return iterator.next();
        }

        Entry<MemorySegment> entry = peekedEntry;
        peekedEntry = null;
        return entry;
    }

    @Override
    public Entry<MemorySegment> peek() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        if (peekedEntry == null) {
            peekedEntry = iterator.next();
        }
        return peekedEntry;
    }
}
