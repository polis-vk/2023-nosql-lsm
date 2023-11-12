package ru.vk.itmo.dyagayalexandra;

import ru.vk.itmo.Entry;

import java.lang.foreign.MemorySegment;
import java.util.Iterator;
import java.util.NoSuchElementException;

public class SkipNullIterator implements Iterator<Entry<MemorySegment>> {

    private final Iterator<Entry<MemorySegment>> iterator;
    private Entry<MemorySegment> current;

    public SkipNullIterator(Iterator<Entry<MemorySegment>> iterator) {
        this.iterator = iterator;
    }

    @Override
    public boolean hasNext() {
        if (current != null) {
            return true;
        }

        while (iterator.hasNext()) {
            Entry<MemorySegment> entry = iterator.next();
            if (entry.value() != null) {
                this.current = entry;
                return true;
            }
        }

        return false;
    }

    @Override
    public Entry<MemorySegment> next() {
        if (!hasNext()) {
            throw new NoSuchElementException("There is no next element.");
        }

        Entry<MemorySegment> next = current;
        current = null;
        return next;
    }
}
