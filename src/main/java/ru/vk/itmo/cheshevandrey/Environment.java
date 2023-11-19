package ru.vk.itmo.cheshevandrey;

import ru.vk.itmo.Entry;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;

public class Environment {

    private final ConcurrentSkipListMap<MemorySegment, Entry<MemorySegment>> table;
    private final ConcurrentSkipListMap<MemorySegment, Entry<MemorySegment>> flushingTable;
    private final AtomicLong memTableBytes;

    private final DiskStorage diskStorage;

    public Environment(ConcurrentSkipListMap<MemorySegment, Entry<MemorySegment>> memtable,
                       ConcurrentSkipListMap<MemorySegment, Entry<MemorySegment>> flushingTable,
                       Path storagePath,
                       Arena arena)
            throws IOException {
        this.diskStorage = new DiskStorage(storagePath, arena);

        this.table = memtable;
        this.flushingTable = flushingTable;
        this.memTableBytes = new AtomicLong(0);
    }

    public Iterator<Entry<MemorySegment>> range(MemorySegment from, MemorySegment to) {
        Iterator<Entry<MemorySegment>> memTableIterator = getInMemory(from, to, true).iterator();
        Iterator<Entry<MemorySegment>> flushingIterator = getInMemory(from, to, false).iterator();

        return diskStorage.range(memTableIterator, flushingIterator, from, to, false);
    }

    private Iterable<Entry<MemorySegment>> getInMemory(MemorySegment from, MemorySegment to, Boolean isMemTable) {
        if (from == null && to == null) {
            return (isMemTable ? table : flushingTable).values();
        }
        if (from == null) {
            return (isMemTable ? table : flushingTable).headMap(to).values();
        }
        if (to == null) {
            return (isMemTable ? table : flushingTable).tailMap(from).values();
        }
        return (isMemTable ? table : flushingTable).subMap(from, to).values();
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

    public void flush() throws IOException {
        diskStorage.save(table.values());
    }

    public void compact() throws IOException {
        diskStorage.compact();
    }

    public void completeCompactIfNeeded() throws IOException {
        diskStorage.completeCompactIfNeeded();
    }

    public ConcurrentSkipListMap<MemorySegment, Entry<MemorySegment>> getTable() {
        return table;
    }

    public AtomicLong getMemTableBytes() {
        return memTableBytes;
    }

    public Entry<MemorySegment> getMemTableEntry(MemorySegment key) {
        return table.get(key);
    }

    public Entry<MemorySegment> getFlushingTableEntry(MemorySegment key) {
        return flushingTable.get(key);
    }
}
