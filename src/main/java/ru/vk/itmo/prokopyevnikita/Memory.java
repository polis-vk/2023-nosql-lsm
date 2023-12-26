package ru.vk.itmo.prokopyevnikita;

import ru.vk.itmo.Entry;

import java.lang.foreign.MemorySegment;
import java.util.Collection;
import java.util.Iterator;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

class Memory {
    //  Placeholder for not presented memory
    static final Memory NOT_PRESENTED = new Memory(-1);
    private final long threshold;
    private final AtomicLong size = new AtomicLong();
    private final AtomicBoolean isOversized = new AtomicBoolean();

    private final NavigableMap<MemorySegment, Entry<MemorySegment>> delegate =
            new ConcurrentSkipListMap<>(MemorySegmentComparator::compare);

    public Memory(long threshold) {
        this.threshold = threshold;
    }

    public Collection<Entry<MemorySegment>> values() {
        return delegate.values();
    }

    public boolean overflow() {
        return !isOversized.getAndSet(true);
    }

    public boolean put(MemorySegment key, Entry<MemorySegment> entry) {
        if (threshold == -1) {
            throw new UnsupportedOperationException("Write not supported");
        }
        // put and get previous value
        Entry<MemorySegment> segmentEntry = delegate.put(key, entry);
        // calculate size of new entry
        long newEntrySize = Storage.getSizeOnDisk(entry);
        // if previous value exists, subtract its size
        if (segmentEntry != null) {
            newEntrySize -= Storage.getSizeOnDisk(segmentEntry);
        }
        // add new entry size to total size
        long newSize = size.addAndGet(newEntrySize);
        // if total size is greater than threshold, return true (make autoFlush)
        if (newSize > threshold) {
            return overflow();
        }
        return false;
    }

    public Entry<MemorySegment> get(MemorySegment key) {
        return delegate.get(key);
    }

    public Iterator<Entry<MemorySegment>> get(MemorySegment from, MemorySegment to) {
        if (from == null && to == null) {
            return delegate.values().iterator();
        } else if (to == null) {
            return delegate.tailMap(from).values().iterator();
        } else if (from == null) {
            return delegate.headMap(to).values().iterator();
        }
        return delegate.subMap(from, to).values().iterator();
    }

    public boolean isEmpty() {
        return delegate.isEmpty();
    }
}
