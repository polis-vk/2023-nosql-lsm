package ru.vk.itmo.bandurinvladislav.iterator;

import ru.vk.itmo.Entry;

import java.lang.foreign.MemorySegment;
import java.util.Iterator;

public interface MemorySegmentIterator extends Iterator<Entry<MemorySegment>> {
    Entry<MemorySegment> peek();
    int getPriority();
}
