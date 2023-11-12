package ru.vk.itmo.cheshevandrey;

import ru.vk.itmo.Config;
import ru.vk.itmo.Entry;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.util.Iterator;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;

public class Environment {

    private ConcurrentSkipListMap<MemorySegment, Entry<MemorySegment>> table;
    private ConcurrentSkipListMap<MemorySegment, Entry<MemorySegment>> flushingTable;
    private final AtomicLong memTableBytes;

    private final DiskStorage diskStorage;
    private final Config config;

    private final Lock readLock;
//    private final Lock writeLock;

    public Environment(ConcurrentSkipListMap<MemorySegment, Entry<MemorySegment>> memtable,
                       Config config,
                       Arena arena,
                       Lock readLock,
                       Lock writeLock)
            throws IOException {
        this.diskStorage = new DiskStorage(DiskStorage.loadOrRecover(config.basePath(), arena));

        this.table = memtable;
        this.flushingTable = new ConcurrentSkipListMap<>(Tools::compare);
        this.memTableBytes = new AtomicLong(0);

        this.readLock = readLock;
//        this.writeLock = writeLock;

        this.config = config;
    }

    public Iterator<Entry<MemorySegment>> range(MemorySegment from, MemorySegment to) {
        Iterator<Entry<MemorySegment>> memTableIterator;
        Iterator<Entry<MemorySegment>> flushingIterator;
        readLock.lock();
        try {
            memTableIterator = getInMemory(from, to, true).iterator();
            flushingIterator = getInMemory(from, to, false).iterator();
        } finally {
            readLock.unlock();
        }
        return diskStorage.range(memTableIterator, flushingIterator, from, to);
    }

    public void finishCompact() {

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
        flushingTable = table;
        table = new ConcurrentSkipListMap<>(Tools::compare);
        memTableBytes.set(0);

        DiskStorage.save(config.basePath(), flushingTable.values());
    }

    public void compact() throws IOException {
        diskStorage.compact(config.basePath());
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
