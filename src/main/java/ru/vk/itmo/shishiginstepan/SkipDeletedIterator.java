package ru.vk.itmo.shishiginstepan;

import ru.vk.itmo.Entry;

import java.lang.foreign.MemorySegment;
import java.util.Comparator;
import java.util.Iterator;

public class SkipDeletedIterator implements Iterator<Entry<MemorySegment>> {
    private Entry<MemorySegment> prefetched;
    private final Iterator<Entry<MemorySegment>> iterator;

    private final Comparator<MemorySegment> comparator;
    private final MemorySegment deletionMark;

    public SkipDeletedIterator(
            Iterator<Entry<MemorySegment>> iterator,
            MemorySegment deletionMark,
            Comparator<MemorySegment> comparator
    ) {
        this.iterator = iterator;
        this.deletionMark = deletionMark;
        this.comparator = comparator;
    }

    @Override
    public boolean hasNext() {
        this.skipDeleted();
        return this.iterator.hasNext() || this.prefetched != null;
    }

    @Override
    public Entry<MemorySegment> next() {
        this.skipDeleted();
        if (this.prefetched == null) {
            return this.iterator.next();
        } else {
            var toReturn = this.prefetched;
            this.prefetched = null;
            return toReturn;
        }
    }

    public Entry<MemorySegment> peekNext() {
        if (this.prefetched == null) {
            this.prefetched = this.iterator.next();
        }
        return this.prefetched;
    }

    public void skipDeleted() {
        if (this.iterator.hasNext()) {
            var next = this.peekNext();
            if (this.comparator.compare(next.value(), deletionMark) == 0) {
                this.prefetched = null;
                this.skipDeleted();
            }
        }
    }
}
