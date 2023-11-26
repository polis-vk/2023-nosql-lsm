package ru.vk.itmo.reference;

import ru.vk.itmo.Entry;

import java.lang.foreign.MemorySegment;
import java.util.Iterator;

/**
 * Represents readable key-value source.
 *
 * @author incubos
 */
public interface ReadableTable {
    Iterator<Entry<MemorySegment>> get(
            MemorySegment from,
            MemorySegment to);

    Entry<MemorySegment> get(MemorySegment key);
}
