package ru.vk.itmo.test.osipovdaniil;

import ru.vk.itmo.Entry;

import java.lang.foreign.MemorySegment;
import java.util.Iterator;
import java.util.NoSuchElementException;

public class SaveMapIterator implements SavingIterator {
    final Iterator<Entry<MemorySegment>> iterator;

    Entry<MemorySegment> currEntry = null;

    public SaveMapIterator(Iterator<Entry<MemorySegment>> iterator) {
        this.iterator = iterator;
        if (iterator.hasNext()) {
            currEntry = iterator.next();
        }
    }

    /**
     * Returns {@code true} if the iteration has more elements.
     * (In other words, returns {@code true} if {@link #next} would
     * return an element rather than throwing an exception.)
     *
     * @return {@code true} if the iteration has more elements
     */
    @Override
    public boolean hasNext() {
        return currEntry != null;
    }

    /**
     * Returns the next element in the iteration.
     *
     * @return the next element in the iteration
     * @throws NoSuchElementException if the iteration has no more elements
     */
    @Override
    public Entry<MemorySegment> next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        final Entry<MemorySegment> res = currEntry;
        currEntry = iterator.hasNext() ? iterator.next() : null;
        return res;
    }

    @Override
    public Entry<MemorySegment> getCurrEntry() {
        return null;
    }
}
