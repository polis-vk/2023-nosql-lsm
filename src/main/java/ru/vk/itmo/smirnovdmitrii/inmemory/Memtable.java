package ru.vk.itmo.smirnovdmitrii.inmemory;

import ru.vk.itmo.Entry;

import java.lang.foreign.MemorySegment;
import java.util.Iterator;
import java.util.concurrent.locks.Lock;

/**
 * Representation of memtable.
 */
public interface Memtable extends Iterable<Entry<MemorySegment>> {

    long size();

    void upsert(Entry<MemorySegment> entry);

    Entry<MemorySegment> get(MemorySegment key);

    Iterator<Entry<MemorySegment>> get(MemorySegment from, MemorySegment to);

    void clear();

    Lock upsertLock();

    Lock flushLock();
}

