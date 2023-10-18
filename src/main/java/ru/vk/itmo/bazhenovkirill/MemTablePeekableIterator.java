package ru.vk.itmo.bazhenovkirill;

import ru.vk.itmo.Entry;

import java.lang.foreign.MemorySegment;
import java.util.Iterator;

public class MemTablePeekableIterator implements Iterator<Entry<MemorySegment>> {

    private final Iterator<Entry<MemorySegment>> iterator;
    private Entry<MemorySegment> current;
    private boolean hasCurrent;

    public MemTablePeekableIterator(Iterator<Entry<MemorySegment>> iterator) {
        this.iterator = iterator;
        if (iterator.hasNext()) {
            current = iterator.next();
            hasCurrent = true;
        } else {
            hasCurrent = false;
        }
    }

    public Entry<MemorySegment> getCurrent() {
        return current;
    }

    public MemorySegment getCurrentKey() {
        return current.key();
    }

    @Override
    public boolean hasNext() {
        return hasCurrent;
    }

    @Override
    public Entry<MemorySegment> next() {
        Entry<MemorySegment> rslt = current;
        if (iterator.hasNext()) {
            current = iterator.next();
        } else {
            hasCurrent = false;
        }
        return rslt;
    }
}
