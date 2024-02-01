package ru.vk.itmo.osipovdaniil;

import ru.vk.itmo.Config;
import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Iterator;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class InMemoryDao implements Dao<MemorySegment, Entry<MemorySegment>> {

    private static final String DATA = "data";

    private final Path path;

    private final Arena arena;

    private final DiskStorage diskStorage;
    private final ConcurrentNavigableMap<MemorySegment, Entry<MemorySegment>> memorySegmentMap
            = new ConcurrentSkipListMap<>(Utils::compareMemorySegments);

    public InMemoryDao(final Config config) throws IOException {
        this.path = config.basePath().resolve(DATA);
        Files.createDirectories(path);
        this.arena = Arena.ofShared();
        this.diskStorage = new DiskStorage(DiskStorageUtils.loadOrRecover(path, arena));
    }

    /**
     * Compacts data (no-op by default).
     */
    @Override
    public void compact() throws IOException {
        DiskStorageUtils.compact(path, this::all);
    }

    /**
     * Returns ordered iterator of entries with keys between from (inclusive) and to (exclusive).
     *
     * @param from lower bound of range (inclusive)
     * @param to   upper bound of range (exclusive)
     * @return entries [from;to)
     */
    @Override
    public Iterator<Entry<MemorySegment>> get(final MemorySegment from, final MemorySegment to) {
        return diskStorage.range(getInMemory(from, to), from, to);
    }

    private Iterator<Entry<MemorySegment>> getInMemory(final MemorySegment from, final MemorySegment to) {
        if (from == null && to == null) {
            return memorySegmentMap.values().iterator();
        }
        if (from == null) {
            return memorySegmentMap.headMap(to).values().iterator();
        }
        if (to == null) {
            return memorySegmentMap.tailMap(from).values().iterator();
        }
        return memorySegmentMap.subMap(from, to).values().iterator();
    }

    /**
     * Returns entry by key. Note: default implementation is far from optimal.
     *
     * @param key entry`s key
     * @return entry
     */
    @Override
    public Entry<MemorySegment> get(final MemorySegment key) {
        final Entry<MemorySegment> entry = memorySegmentMap.get(key);
        if (entry != null) {
            if (entry.value() == null) {
                return null;
            }
            return entry;
        }

        final Iterator<Entry<MemorySegment>> iterator = diskStorage.range(Collections.emptyIterator(), key, null);

        if (!iterator.hasNext()) {
            return null;
        }
        Entry<MemorySegment> next = iterator.next();
        if (next.key().mismatch(key) == -1) {
            return next;
        }
        return null;
    }

    /**
     * Inserts of replaces entry.
     *
     * @param entry element to upsert
     */
    @Override
    public void upsert(final Entry<MemorySegment> entry) {
        memorySegmentMap.put(entry.key(), entry);
    }

    /**
     * Persists data (no-op by default).
     */
    @Override
    public void flush() throws IOException {
        if (!memorySegmentMap.isEmpty()) {
            DiskStorageUtils.save(path, memorySegmentMap.values());
        }
    }

    @Override
    public void close() throws IOException {
        if (!arena.scope().isAlive()) {
            return;
        }
        arena.close();
        flush();
    }
}
