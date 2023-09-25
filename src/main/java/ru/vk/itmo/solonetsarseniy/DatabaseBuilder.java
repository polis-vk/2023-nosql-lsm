package ru.vk.itmo.solonetsarseniy;

import ru.vk.itmo.Entry;

import java.lang.foreign.MemorySegment;
import java.util.concurrent.ConcurrentSkipListMap;

public class DatabaseBuilder {
    private final static MemorySegmentComparator COMPARATOR = new MemorySegmentComparator();

    public static ConcurrentSkipListMap<MemorySegment, Entry<MemorySegment>> build() {
        return new ConcurrentSkipListMap<>(COMPARATOR);
    }
}
