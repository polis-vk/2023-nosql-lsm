package ru.vk.itmo.boturkhonovkamron.persistence;

import java.lang.foreign.MemorySegment;

public record SSTableFileMap(MemorySegment indexMap, MemorySegment tableMap) {
}
