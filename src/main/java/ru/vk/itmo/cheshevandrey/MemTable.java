package ru.vk.itmo.cheshevandrey;

import ru.vk.itmo.Entry;

import java.lang.foreign.MemorySegment;
import java.util.Iterator;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

class MemTable implements Iterable<Entry<MemorySegment>> {

    private ConcurrentSkipListMap<MemorySegment, Entry<MemorySegment>> table;
    private ConcurrentSkipListMap<MemorySegment, Entry<MemorySegment>> flushingTable;
    private final AtomicBoolean isFlushing;
    private final AtomicLong memTableBytes;

    MemTable() {
        this.table = new ConcurrentSkipListMap<>(Tools::compare);
        this.flushingTable = new ConcurrentSkipListMap<>(Tools::compare);
        this.memTableBytes = new AtomicLong(0);
        this.isFlushing = new AtomicBoolean(false);
    }

    public synchronized long put(Entry<MemorySegment> entry) {
        table.put(entry.key(), entry);

        long currSize = entry.key().byteSize();
        if (entry.value() != null) {
            currSize += entry.value().byteSize();
        }

        memTableBytes.getAndAdd(currSize);
        return memTableBytes.get();
    }

    public Iterable<Entry<MemorySegment>> startFlush() {
        flushingTable = table;
        table = new ConcurrentSkipListMap<>(Tools::compare);
        memTableBytes.set(0);
        isFlushing.set(true);
        return flushingTable.values();
    }

    public void finishFlush() {
        isFlushing.set(false);
        flushingTable = new ConcurrentSkipListMap<>(Tools::compare);
    }

    public AtomicBoolean isFlushing() {
        return isFlushing;
    }

    public Entry<MemorySegment> getMemTableEntry(MemorySegment key) {
        return table.get(key);
    }

    public Entry<MemorySegment> getFlushingTableEntry(MemorySegment key) {
        return flushingTable.get(key);
    }

    public ConcurrentSkipListMap<MemorySegment, Entry<MemorySegment>> getFlushingTable() {
        return flushingTable;
    }

    public ConcurrentSkipListMap<MemorySegment, Entry<MemorySegment>> getTable() {
        return table;
    }

    public AtomicLong getBytes() {
        return memTableBytes;
    }

    @Override
    public Iterator<Entry<MemorySegment>> iterator() {
        return table.values().iterator();
    }
}
