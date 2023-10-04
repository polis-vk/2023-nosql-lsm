package ru.vk.itmo.bazhenovkirill.strategy;

import ru.vk.itmo.Entry;

import java.lang.foreign.MemorySegment;

public interface ElementSearchStrategy {

    Entry<MemorySegment> search(MemorySegment data, MemorySegment key, long fileSize);

}
