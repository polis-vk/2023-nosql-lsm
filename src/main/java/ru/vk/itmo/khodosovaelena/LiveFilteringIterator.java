package ru.vk.itmo.khodosovaelena;

import ru.vk.itmo.Entry;

import java.lang.foreign.MemorySegment;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Filters non tombstone {@link Entry}s.
 *
 * @author incubos
 */
final class LiveFilteringIterator<T extends Entry<MemorySegment>> implements Iterator<T> {
    private final Iterator<T> delegate;
    private T next;

    LiveFilteringIterator(final Iterator<T> delegate) {
        this.delegate = delegate;
        skipTombstones();
    }

    private void skipTombstones() {
        while (delegate.hasNext()) {
            final T entry = delegate.next();
            if (entry.value() != null) {
                this.next = entry;
                break;
            }
        }
    }

    @Override
    public boolean hasNext() {
        return next != null;
    }

    @Override
    public T next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }

        // Consume
        final T result = next;
        next = null;

        skipTombstones();

        return result;
    }
}
