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
final class LiveFilteringIterator implements Iterator<TimeStampEntry> {
    private final Iterator<TimeStampEntry> delegate;
    private TimeStampEntry next;

    LiveFilteringIterator(final Iterator<TimeStampEntry> delegate) {
        this.delegate = delegate;
        skipTombstones();
    }

    private void skipTombstones() {
        while (delegate.hasNext()) {
            final TimeStampEntry entry = delegate.next();
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
    public TimeStampEntry next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }

        // Consume
        final TimeStampEntry result = next;
        next = null;

        skipTombstones();

        return result;
    }
}
