package ru.vk.itmo.solonetsarseniy.helpers;

import ru.vk.itmo.Entry;

import java.lang.foreign.MemorySegment;
import java.util.concurrent.ConcurrentSkipListMap;

public class DatabaseBuilder {
    private static final MemorySegmentComparator COMPARATOR = new MemorySegmentComparator();

    public ConcurrentSkipListMap<MemorySegment, Entry<MemorySegment>> build() {
        return new ConcurrentSkipListMap<>(COMPARATOR);
    }
}
