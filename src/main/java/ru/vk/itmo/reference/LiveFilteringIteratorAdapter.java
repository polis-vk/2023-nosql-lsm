package ru.vk.itmo.reference;

import ru.vk.itmo.Entry;

import java.lang.foreign.MemorySegment;
import java.util.Iterator;

public class LiveFilteringIteratorAdapter implements Iterator<Entry<MemorySegment>> {
    private final LiveFilteringIterator liveFilteringIterator;

    public LiveFilteringIteratorAdapter(LiveFilteringIterator liveFilteringIterator) {
        this.liveFilteringIterator = liveFilteringIterator;
    }

    @Override
    public boolean hasNext() {
        return liveFilteringIterator.hasNext();
    }

    @Override
    public Entry<MemorySegment> next() {
        return liveFilteringIterator.next().getClearEntry();
    }
}
