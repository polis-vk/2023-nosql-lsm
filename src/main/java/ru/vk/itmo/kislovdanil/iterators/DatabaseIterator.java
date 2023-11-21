package ru.vk.itmo.kislovdanil.iterators;

import ru.vk.itmo.Entry;

import java.lang.foreign.MemorySegment;
import java.util.Iterator;

public interface DatabaseIterator extends Iterator<Entry<MemorySegment>> {
    long getPriority();
}
