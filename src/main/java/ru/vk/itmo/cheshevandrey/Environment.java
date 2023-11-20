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

    private ConcurrentSkipListMap<MemorySegment, Entry<MemorySegment>> memTable;
    private ConcurrentSkipListMap<MemorySegment, Entry<MemorySegment>> flushingTable;
    private AtomicLong memTableBytes;

    private final DiskStorage diskStorage;

    public Environment(
            ConcurrentSkipListMap<MemorySegment, Entry<MemorySegment>> table,
            long bytes,
            Path storagePath,
            Arena arena
    ) throws IOException {
        this.diskStorage = new DiskStorage(storagePath, arena);
        this.memTable = table;
        this.flushingTable = new ConcurrentSkipListMap<>(Tools::compare);
        this.memTableBytes = new AtomicLong(bytes);
    }

    public Iterator<Entry<MemorySegment>> range(MemorySegment from, MemorySegment to) {
        Iterator<Entry<MemorySegment>> memTableIterator = getInMemory(from, to, true).iterator();
        Iterator<Entry<MemorySegment>> flushingIterator = getInMemory(from, to, false).iterator();

        return diskStorage.range(memTableIterator, flushingIterator, from, to, Range.ALL);
    }

    private Iterable<Entry<MemorySegment>> getInMemory(MemorySegment from, MemorySegment to, Boolean isMemTable) {
        if (from == null && to == null) {
            return (isMemTable ? memTable : flushingTable).values();
        }
        if (from == null) {
            return (isMemTable ? memTable : flushingTable).headMap(to).values();
        }
        if (to == null) {
            return (isMemTable ? memTable : flushingTable).tailMap(from).values();
        }
        return (isMemTable ? memTable : flushingTable).subMap(from, to).values();
    }

    public synchronized long put(Entry<MemorySegment> entry) {
        memTable.put(entry.key(), entry);

        long currSize = entry.key().byteSize();
        if (entry.value() != null) {
            currSize += entry.value().byteSize();
        }

        memTableBytes.getAndAdd(currSize);
        return memTableBytes.get();
    }

    public void flush() throws IOException {
        this.flushingTable = memTable;
        this.memTable = new ConcurrentSkipListMap<>(Tools::compare);
        this.memTableBytes = new AtomicLong(0);

        diskStorage.save(flushingTable.values());
    }

    public void compact() throws IOException {
        diskStorage.compact();
    }

    public void completeCompactIfNeeded() throws IOException {
        diskStorage.completeCompactIfNeeded();
    }

    public ConcurrentSkipListMap<MemorySegment, Entry<MemorySegment>> getTable() {
        return memTable;
    }

    public long getBytes() {
        return memTableBytes.get();
    }

    public Entry<MemorySegment> getMemTableEntry(MemorySegment key) {
        return memTable.get(key);
    }

    public Entry<MemorySegment> getFlushingTableEntry(MemorySegment key) {
        return flushingTable.get(key);
    }
}
