package ru.vk.itmo.cheshevandrey;

import ru.vk.itmo.Config;
import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class InMemoryDao implements Dao<MemorySegment, Entry<MemorySegment>> {

    private final Arena arena;
    private final DiskStorage diskStorage;
    private final Path path;
    private final long flushThresholdBytes;
    private final ExecutorService executor;

    private final ReadWriteLock lock;
    private final Lock readLock;
    private final Lock writeLock;

    private MemTable memTable;
    private final AtomicBoolean shouldFlushAgain;

    public InMemoryDao(Config config) throws IOException {
        this.flushThresholdBytes = config.flushThresholdBytes();
        this.path = config.basePath().resolve("data");
        Files.createDirectories(path);

        this.lock = new ReentrantReadWriteLock();
        this.readLock = lock.readLock();
        this.writeLock = lock.writeLock();

        this.executor = Executors.newCachedThreadPool();

        this.memTable = new MemTable();
        this.shouldFlushAgain = new AtomicBoolean(false);

        arena = Arena.ofShared();
        this.diskStorage = new DiskStorage(DiskStorage.loadOrRecover(path, arena));

    }

    @Override
    public Iterator<Entry<MemorySegment>> get(MemorySegment from, MemorySegment to) {
        Iterator<Entry<MemorySegment>> memTableIter = getInMemory(from, to).iterator();
        return diskStorage.range(memTableIter, memTable.getFlushingTable().values().iterator(), from, to);
    }

    private Iterable<Entry<MemorySegment>> getInMemory(MemorySegment from, MemorySegment to) {
        if (from == null && to == null) {
            return memTable.getTable().values();
        }
        if (from == null) {
            return memTable.getTable().headMap(to).values();
        }
        if (to == null) {
            return memTable.getTable().tailMap(from).values();
        }
        return memTable.getTable().subMap(from, to).values();
    }

    @Override
    public void upsert(Entry<MemorySegment> entry) {
        MemTable currMemtable;
        readLock.lock();
        try {
            currMemtable = memTable;
            if (currMemtable.getBytes().get() > flushThresholdBytes && memTable.isFlushing().get()) {
                throw new IllegalStateException("table is full, flushing in process");
            }
        } finally {
            readLock.unlock();
        }

        if (currMemtable.put(entry) <= flushThresholdBytes) {
            return;
        }

        executor.execute(() -> {
            try {
                flush();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
    }

    @Override
    public Entry<MemorySegment> get(MemorySegment key) {
        Entry<MemorySegment> memTableEntry;
        Entry<MemorySegment> flushingTableEntry;
        readLock.lock();
        try {
            memTableEntry = memTable.getMemTableEntry(key);
            flushingTableEntry = memTable.getFlushingTableEntry(key);
        } finally {
            readLock.unlock();
        }

        if (memTableEntry != null) {
            return Tools.entryToReturn(memTableEntry);
        } else if (flushingTableEntry != null) {
            return Tools.entryToReturn(flushingTableEntry);
        }

        Iterator<Entry<MemorySegment>> iterator = diskStorage.range(
                getInMemory(null, null).iterator(),
                memTable.getFlushingTable().values().iterator(),
                null,
                null
        );

        if (!iterator.hasNext()) {
            return null;
        }
        Entry<MemorySegment> next = iterator.next();
        if (Tools.compare(next.key(), key) == 0) {
            return next;
        }
        return null;
    }

    /**
     * Если compact был вызван во время выполнения другого compact и во время выполнения другого
     * произошел flush новой sstable, то текущий запрашиваемый compact будет выполнен после выполняющегося.
     * Тем самым гарантируем, что sstabl'ы, которые были не в процессе compact'а во время вызова нового
     * рано или поздно будут скомпакчены.
     */
    @Override
    public void compact() throws IOException {
        executor.execute(() -> {
            try {
                diskStorage.compact(path);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
    }

    /**
     * Если flush был вызван во время выполнения другого flush и memtable на этот
     * момент была не пустая, то текущий запрашиваемый будет выполнен после выполняющегося.
     * Тем самым гарантируем, что данные, которые были в memtable на момент вызова
     * flush рано или поздно окажутся на диске.
     */
    @Override
    public void flush() throws IOException {
        readLock.lock();
        try {
            if (memTable.getTable().isEmpty()) {
                return;
            } else if (memTable.isFlushing().get()) {
                // Считаем, что после выполнения текущего должны сделать очередной flush.
                shouldFlushAgain.set(true);
                return;
            }
        } finally {
            readLock.unlock();
        }

        do {
            Iterable<Entry<MemorySegment>> flushingTable;
            writeLock.lock();
            try {
                shouldFlushAgain.set(false);
                flushingTable = memTable.startFlush();
            } finally {
                writeLock.unlock();
            }

            DiskStorage.save(path, flushingTable);

            writeLock.lock();
            try {
                memTable.finishFlush();
            } finally {
                writeLock.unlock();
            }
        } while (shouldFlushAgain.get());
    }

    @Override
    public void close() throws IOException {
        // Ожидаем выполнения фоновых flush и сompact.
        executor.close();

        if (!arena.scope().isAlive()) {
            return;
        }
        arena.close();

        boolean needsFlush;
        readLock.lock();
        try {
            needsFlush = !memTable.getTable().isEmpty();
        } finally {
            readLock.unlock();
        }

        if (needsFlush) {
            flush();
        }
    }
}
