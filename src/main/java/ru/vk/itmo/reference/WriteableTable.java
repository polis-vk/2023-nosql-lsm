package ru.vk.itmo.reference;

import ru.vk.itmo.Entry;

import java.lang.foreign.MemorySegment;

/**
 * Writeable data store.
 *
 * @author incubos
 */
public interface WriteableTable {
    void upsert(Entry<MemorySegment> entry);
}
