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

    final AtomicBoolean isFlushing;
    final AtomicBoolean isCompacting;
    final AtomicBoolean isFlushingCompleted;
    final AtomicBoolean isCompactingCompleted;

    private final Lock readLock;
    private final Lock writeLock;

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
        this.writeLock = writeLock;

        this.config = config;

        this.isFlushing = new AtomicBoolean(false);
        this.isCompacting = new AtomicBoolean(false);
        this.isFlushingCompleted = new AtomicBoolean(false);
        this.isCompactingCompleted = new AtomicBoolean(false);
    }

    public Iterator<Entry<MemorySegment>> range(MemorySegment from, MemorySegment to) {
        return diskStorage.range(table.values().iterator(), flushingTable.values().iterator(), from, to);
    }

    public long put(Entry<MemorySegment> entry) {
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

        isFlushing.set(true);

        DiskStorage.save(config.basePath(), flushingTable.values());

        isFlushing.set(false);
        isFlushingCompleted.set(true);
    }

    public void compact() throws IOException {
        isCompacting.set(true);

        diskStorage.compact(config.basePath());

        isCompacting.set(false);
        isCompactingCompleted.set(true);
    }

    public ConcurrentSkipListMap<MemorySegment, Entry<MemorySegment>> getTable() {
        return table;
    }

    public ConcurrentSkipListMap<MemorySegment, Entry<MemorySegment>> getFlushingTable() {
        return flushingTable;
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
