package ru.vk.itmo.proninvalentin.iterators;

import ru.vk.itmo.Entry;

import java.lang.foreign.MemorySegment;
import java.util.Iterator;

public interface PeekingPriorityIterator extends Iterator<Entry<MemorySegment>>, Comparable<PeekingPriorityIterator> {
    Entry<MemorySegment> getCurrent();

    int getPriority();
}
