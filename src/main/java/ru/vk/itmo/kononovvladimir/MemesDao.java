package ru.vk.itmo.kononovvladimir;

import ru.vk.itmo.Config;
import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class MemesDao implements Dao<MemorySegment, Entry<MemorySegment>> {

    private static class State {

        private final NavigableMap<MemorySegment, Entry<MemorySegment>> memoryStorage;
        //private final NavigableMap<MemorySegment, Entry<MemorySegment>> flushingMemoryTable;
        //private final AtomicLong memoryStorageSizeInBytes;
        private final DiskStorage diskStorage;

        private State(NavigableMap<MemorySegment, Entry<MemorySegment>> memoryStorage,
                      NavigableMap<MemorySegment, Entry<MemorySegment>> flushingMemoryTable,
                      DiskStorage diskStorage) {
            this.memoryStorage = memoryStorage;
            //this.flushingMemoryTable = flushingMemoryTable;
            //убрать
            if (flushingMemoryTable == null || flushingMemoryTable.isEmpty()) {
                //this.memoryStorageSizeInBytes = new AtomicLong();
                this.diskStorage = diskStorage;
            } else this.diskStorage = null;
        }
    }

    private final Comparator<MemorySegment> comparator = MemesDao::compare;
    private final Arena arena;
    private final Path path;
    private final AtomicBoolean isClosed = new AtomicBoolean(false);
    //private final ReadWriteLock memoryLock = new ReentrantReadWriteLock();
    //private final Lock lock = new ReentrantLock();
    //private final long flushThresholdBytes;
    private State state;

    public MemesDao(Config config) throws IOException {
        this.path = config.basePath().resolve("data");
        Files.createDirectories(path);

        //this.flushThresholdBytes = config.flushThresholdBytes();
        this.arena = Arena.ofShared();
        this.state = new State(
                new ConcurrentSkipListMap<>(comparator),
                null,
                new DiskStorage(DiskStorage.loadOrRecover(path, arena))
        );
    }

    static int compare(MemorySegment memorySegment1, MemorySegment memorySegment2) {
        long mismatch = memorySegment1.mismatch(memorySegment2);
        if (mismatch == -1) {
            return 0;
        }

        if (mismatch == memorySegment1.byteSize()) {
            return -1;
        }

        if (mismatch == memorySegment2.byteSize()) {
            return 1;
        }
        byte b1 = memorySegment1.get(ValueLayout.JAVA_BYTE, mismatch);
        byte b2 = memorySegment2.get(ValueLayout.JAVA_BYTE, mismatch);
        return Byte.compare(b1, b2);
    }

    @Override
    public Iterator<Entry<MemorySegment>> get(MemorySegment from, MemorySegment to) {
        return state.diskStorage.range(getInMemory(from, to), from, to);
    }

    private Iterator<Entry<MemorySegment>> getInMemory(MemorySegment from, MemorySegment to) {
        if (from == null && to == null) {
            return state.memoryStorage.values().iterator();
        }
        if (from == null) {
            return state.memoryStorage.headMap(to).values().iterator();
        }
        if (to == null) {
            return state.memoryStorage.tailMap(from).values().iterator();
        }
        return state.memoryStorage.subMap(from, to).values().iterator();
    }

    @Override
    public void upsert(Entry<MemorySegment> entry) {
        if (isClosed.get()) {
            //throw
            return;
        }
        state.memoryStorage.put(entry.key(), entry);
       // long entrySize = calculateSize(entry);
/*        if (flushThresholdBytes < state.memoryStorageSizeInBytes.get() + entrySize) {
            // if not flushing throw

            memoryLock.writeLock().lock();
            try {
                state.memoryStorage.put(entry.key(), entry);
                state.memoryStorageSizeInBytes.addAndGet(2 * Long.BYTES + entry.key().byteSize() + entry.value().byteSize());
            } finally {
                memoryLock.writeLock().unlock();
            }
        }*/
        //If very big

    }



    @Override
    public Entry<MemorySegment> get(MemorySegment key) {
        Entry<MemorySegment> entry = state.memoryStorage.get(key);
        if (entry != null) {
            if (entry.value() == null) {
                return null;
            }
            return entry;
        }

        Iterator<Entry<MemorySegment>> iterator = state.diskStorage.range(Collections.emptyIterator(), key, null);

        if (!iterator.hasNext()) {
            return null;
        }
        Entry<MemorySegment> next = iterator.next();
        if (compare(next.key(), key) == 0) {
            return next;
        }
        return null;
    }

    @Override
    public void compact() throws IOException {
        DiskStorage.compact(path, this::all);
    }

    @Override
    public void close() throws IOException {
        if (!arena.scope().isAlive()) {
            return;
        }

        arena.close();

        if (!state.memoryStorage.isEmpty()) {
            DiskStorage.saveNextSSTable(path, state.memoryStorage.values());
        }
    }
}

/*    private Long calculateSize(Entry<MemorySegment> entry){
        return Long.BYTES + entry.key().byteSize() + Long.BYTES
                + (entry.value() == null ? 0 : entry.key().byteSize());
    }*/

/*    @Override
    public void flush() throws IOException {
        memoryLock.writeLock().lock();
        try {
            lock.lock();
            try {
                if (!state.memoryStorage.isEmpty()) {
                    DiskStorage.saveNextSSTable(path, state.memoryStorage.values());
                }
            } finally {
                lock.unlock();
            }
        } finally {
            memoryLock.writeLock().unlock();
        }
        state = new State(
                new ConcurrentSkipListMap<>(comparator),
                state.flushingMemoryTable,
                new DiskStorage(DiskStorage.loadOrRecover(path, arena))
        );
    }*/

/*    private void tryToFlush() {
        try {
            state.diskStorage.flush(state.flushingMemoryTable.values());
            lock.writeLock().lock();
            try {
                state = new State(state.memoryStorage, null, state.diskStorage);
            } finally {
                lock.writeLock().unlock();
            }
        } catch (IOException e) {
            throw new ApplicationException("Can't flush memory table", e);
        }
    }*/