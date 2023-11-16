package ru.vk.itmo.smirnovdmitrii.inmemory;

import ru.vk.itmo.Entry;

import java.io.Closeable;
import java.lang.foreign.MemorySegment;
import java.util.Iterator;

public interface Memtable extends AutoCloseable, Iterable<Entry<MemorySegment>> {

    long size();

    void upsert(Entry<MemorySegment> entry);

    Entry<MemorySegment> get(MemorySegment key);

    Iterator<Entry<MemorySegment>> get(MemorySegment from, MemorySegment to);

    boolean isAlive();

    void kill();

    long writers();

    boolean tryOpen();

    @Override
    void close();
}

