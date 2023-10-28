package ru.vk.itmo.boturkhonovkamron;

import ru.vk.itmo.Config;
import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;
import ru.vk.itmo.pashchenkoalexandr.DiskStorage;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Iterator;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

import static ru.vk.itmo.boturkhonovkamron.MemorySegmentComparator.COMPARATOR;

/**
 * Implementation of Dao interface for in-memory storage of MemorySegment objects.
 *
 * @author Kamron Boturkhonov
 * @since 2023.09.26
 */
public class InMemoryDao implements Dao<MemorySegment, Entry<MemorySegment>> {

    private final NavigableMap<MemorySegment, Entry<MemorySegment>> storage = new ConcurrentSkipListMap<>(COMPARATOR);

    private final Arena arena;

    private final DiskStorage diskStorage;

    private final Path path;

    public InMemoryDao(final Config config) throws IOException {
        this.path = config.basePath().resolve("data");
        Files.createDirectories(path);

        arena = Arena.ofShared();

        this.diskStorage = new DiskStorage(DiskStorage.loadOrRecover(path, arena));
    }

    @Override
    public Iterator<Entry<MemorySegment>> get(MemorySegment from, MemorySegment to) {
        return diskStorage.range(getInMemory(from, to), from, to);
    }

    private Iterator<Entry<MemorySegment>> getInMemory(MemorySegment from, MemorySegment to) {
        if (from == null && to == null) {
            return storage.values().iterator();
        }
        if (from == null) {
            return storage.headMap(to).values().iterator();
        }
        if (to == null) {
            return storage.tailMap(from).values().iterator();
        }
        return storage.subMap(from, to).values().iterator();
    }

    @Override
    public void upsert(Entry<MemorySegment> entry) {
        storage.put(entry.key(), entry);
    }

    @Override
    public Entry<MemorySegment> get(MemorySegment key) {
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
        if (COMPARATOR.compare(next.key(), key) == 0) {
            return next;
        }
        return null;
    }

    @Override
    public void close() throws IOException {
        if (!arena.scope().isAlive()) {
            return;
        }

        arena.close();

        if (!storage.isEmpty()) {
            DiskStorage.save(path, storage.values());
        }
    }
}
