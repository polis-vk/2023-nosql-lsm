package ru.vk.itmo.reference;

import ru.vk.itmo.Entry;

import java.lang.foreign.MemorySegment;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Filters non tombstone {@link Entry}s.
 *
 * @author incubos
 */
final class LiveFilteringIterator implements Iterator<Entry<MemorySegment>> {
    private final Iterator<Entry<MemorySegment>> delegate;
    private Entry<MemorySegment> next = null;

    LiveFilteringIterator(final Iterator<Entry<MemorySegment>> delegate) {
        this.delegate = delegate;
        skipTombstones();
    }

    private void skipTombstones() {
        while (delegate.hasNext()) {
            final Entry<MemorySegment> next = delegate.next();
            if (next.value() != null) {
                this.next = next;
                break;
            }
        }
    }

    @Override
    public boolean hasNext() {
        return next != null;
    }

    @Override
    public Entry<MemorySegment> next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }

        // Consume
        final Entry<MemorySegment> result = next;
        next = null;

        skipTombstones();

        return result;
    }
}
