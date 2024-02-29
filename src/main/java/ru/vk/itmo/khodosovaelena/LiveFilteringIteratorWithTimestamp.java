package ru.vk.itmo.khodosovaelena;

import ru.vk.itmo.Entry;

import java.lang.foreign.MemorySegment;
import java.util.Iterator;
import java.util.NoSuchElementException;

final class LiveFilteringIteratorWithTimestamp implements Iterator<EntryWithTimestamp<MemorySegment>> {
    private final Iterator<EntryWithTimestamp<MemorySegment>> delegate;
    private EntryWithTimestamp<MemorySegment> next;

    LiveFilteringIteratorWithTimestamp(final Iterator<EntryWithTimestamp<MemorySegment>> delegate) {
        this.delegate = delegate;
        skipTombstones();
    }

    private void skipTombstones() {
        while (delegate.hasNext()) {
            final EntryWithTimestamp<MemorySegment> entry = delegate.next();
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
    public EntryWithTimestamp<MemorySegment> next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }

        // Consume
        final EntryWithTimestamp<MemorySegment> result = next;
        next = null;

        skipTombstones();

        return result;
    }
}
