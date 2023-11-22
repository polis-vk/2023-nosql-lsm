package ru.vk.itmo.solnyshkoksenia;

import ru.vk.itmo.Config;
import ru.vk.itmo.Entry;
import ru.vk.itmo.solnyshkoksenia.storage.DiskStorage;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class State {
    private static final Comparator<MemorySegment> comparator = new MemorySegmentComparator();
    protected final Config config;
    protected final NavigableMap<MemorySegment, Entry<MemorySegment>> storage;
    protected final NavigableMap<MemorySegment, Entry<MemorySegment>> flushingStorage;
    protected final DiskStorage diskStorage;
    protected final AtomicLong storageByteSize = new AtomicLong();
    protected final AtomicBoolean isClosed = new AtomicBoolean();
    protected final AtomicBoolean overflow = new AtomicBoolean();

    public State(Config config,
                 NavigableMap<MemorySegment, Entry<MemorySegment>> storage,
                 NavigableMap<MemorySegment, Entry<MemorySegment>> flushingStorage,
                 DiskStorage diskStorage) {
        this.config = config;
        this.storage = storage;
        this.flushingStorage = flushingStorage;
        this.diskStorage = diskStorage;
    }

    public State(Config config,
                 DiskStorage diskStorage) {
        this.config = config;
        this.storage = new ConcurrentSkipListMap<>(comparator);
        this.flushingStorage = new ConcurrentSkipListMap<>(comparator);
        this.diskStorage = diskStorage;
    }

    public void putInMemory(Entry<MemorySegment> entry) {
        Entry<MemorySegment> previousEntry = storage.put(entry.key(), entry);
        if (previousEntry != null) {
            storageByteSize.addAndGet(-getSize(previousEntry));
        }

        if (storageByteSize.addAndGet(getSize(entry)) > config.flushThresholdBytes()) {
            overflow.set(true);
        }
    }

    public void save() throws IOException {
        diskStorage.save(storage.values());
    }

    private static long getSize(Entry<MemorySegment> entry) {
        long valueSize = entry.value() == null ? 0 : entry.value().byteSize();
        return Long.BYTES + entry.key().byteSize() + Long.BYTES + valueSize;
    }

    public State checkAndGet() {
        if (isClosed.get()) {
            throw new DaoException("Dao is already closed");
        }
        return this;
    }

    public boolean isClosed() {
        return isClosed.get();
    }

    public boolean isOverflowed() {
        return overflow.get();
    }

    public boolean isFlushing() {
        return !flushingStorage.isEmpty();
    }

    public State moveStorage() {
        return new State(config, new ConcurrentSkipListMap<>(comparator), storage, diskStorage);
    }

    public void flush() throws IOException {
        if (flushingStorage.isEmpty()) {
            return;
        }
        diskStorage.save(flushingStorage.values());
    }

    public State close() {
        isClosed.set(true);
        return this;
    }

    public Entry<MemorySegment> get(MemorySegment key, Comparator<MemorySegment> comparator) {
        Entry<MemorySegment> entry = storage.get(key);
        if (entry != null) {
            if (entry.value() == null) {
                return null;
            }
            return entry;
        }

        Iterator<Entry<MemorySegment>> iterator = diskStorage.range(Collections.emptyIterator(), key, null);

        if (!iterator.hasNext()) {
            return null;
        }
        Entry<MemorySegment> next = iterator.next();
        if (comparator.compare(next.key(), key) == 0 && next.value() != null) {
            return next;
        }
        return null;
    }

    protected Iterator<Entry<MemorySegment>> getInMemory(NavigableMap<MemorySegment, Entry<MemorySegment>> memory,
                                                         MemorySegment from, MemorySegment to) {
        if (from == null && to == null) {
            return memory.values().iterator();
        }
        if (from == null) {
            return memory.headMap(to).values().iterator();
        }
        if (to == null) {
            return memory.tailMap(from).values().iterator();
        }
        return memory.subMap(from, to).values().iterator();
    }
}
