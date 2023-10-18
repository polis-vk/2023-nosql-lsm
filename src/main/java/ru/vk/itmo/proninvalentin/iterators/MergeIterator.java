package ru.vk.itmo.proninvalentin.iterators;

import ru.vk.itmo.BaseEntry;

import java.lang.foreign.MemorySegment;
import java.util.Iterator;
import java.util.PriorityQueue;

public class MergeIterator implements Iterator<BaseEntry<MemorySegment>> {
    @Override
    public boolean hasNext() {
        return false;
    }

    @Override
    public BaseEntry<MemorySegment> next() {
        return null;
    }
}
