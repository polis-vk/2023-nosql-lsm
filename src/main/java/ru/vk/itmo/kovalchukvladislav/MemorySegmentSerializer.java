package ru.vk.itmo.kovalchukvladislav;

import ru.vk.itmo.Entry;
import java.lang.foreign.MemorySegment;

public interface MemorySegmentSerializer<D, E extends Entry<D>> {
    D toValue(MemorySegment input);

    MemorySegment fromValue(D value);

    long size(D value);

    E createEntry(D key, D value);
}
