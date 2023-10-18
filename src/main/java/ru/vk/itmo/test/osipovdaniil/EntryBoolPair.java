package ru.vk.itmo.test.osipovdaniil;

import ru.vk.itmo.Entry;

import java.lang.foreign.MemorySegment;

public record EntryBoolPair(Entry<MemorySegment> entry, boolean trueVal) {
}
