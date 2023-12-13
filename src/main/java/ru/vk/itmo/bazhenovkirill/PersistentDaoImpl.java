package ru.vk.itmo.bazhenovkirill;

import ru.vk.itmo.Config;
import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class PersistentDaoImpl implements Dao<MemorySegment, Entry<MemorySegment>> {
    private final ConcurrentNavigableMap<MemorySegment, Entry<MemorySegment>> memTable
            = new ConcurrentSkipListMap<>(new MemorySegmentComparator());
    private final Arena arena;

    private final Path dataPath;
    private final Storage storage;

    public PersistentDaoImpl(Config config) throws IOException {
        dataPath = config.basePath().resolve("data");
        if (!Files.exists(dataPath)) {
            Files.createDirectories(dataPath);
        }

        arena = Arena.ofShared();
        storage = new Storage(Storage.loadData(dataPath, arena));
    }

    @Override
    public Iterator<Entry<MemorySegment>> get(MemorySegment from, MemorySegment to) {
        return storage.range(getInMemTable(from, to), from, to);
    }

    @Override
    public Entry<MemorySegment> get(MemorySegment key) {
        Entry<MemorySegment> entry = memTable.get(key);
        if (entry == null) {
            return storage.get(key);
        }
        return entry.value() == null ? null : entry;
    }

    @Override
    public void upsert(Entry<MemorySegment> entry) {
        memTable.put(entry.key(), entry);
    }

    @Override
    public void flush() throws IOException {
        if (!memTable.isEmpty()) {
            Storage.save(dataPath, memTable.values());
        }
    }

    @Override
    public void compact() throws IOException {
        if (storage.compact(dataPath, this::all)) {
            memTable.clear();
        }
    }

    @Override
    public void close() throws IOException {
        flush();
        if (!arena.scope().isAlive()) {
            return;
        }
        arena.close();
    }

    private Iterator<Entry<MemorySegment>> getInMemTable(MemorySegment from, MemorySegment to) {
        if (from == null) {
            if (to != null) {
                return memTable.headMap(to).values().iterator();
            }
            return memTable.values().iterator();
        } else {
            if (to == null) {
                return memTable.tailMap(from).values().iterator();
            }
            return memTable.subMap(from, true, to, false).values().iterator();
        }
    }
}
