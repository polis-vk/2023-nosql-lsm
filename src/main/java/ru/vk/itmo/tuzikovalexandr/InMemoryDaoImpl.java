package ru.vk.itmo.tuzikovalexandr;

import ru.vk.itmo.Config;
import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class InMemoryDaoImpl implements Dao<MemorySegment, Entry<MemorySegment>> {
    private final NavigableMap<MemorySegment, Entry<MemorySegment>> memory =
            new ConcurrentSkipListMap<>(MemorySegmentComparator::compare);
    private final SSTable ssTable;
    private final Arena arena;
    private final Path path;

    public InMemoryDaoImpl(Config config) throws IOException {
        this.path = config.basePath().resolve("data");
        Files.createDirectories(path);

        arena = Arena.ofShared();

        this.ssTable = new SSTable(SSTable.loadData(path, arena));
    }

    @Override
    public Iterator<Entry<MemorySegment>> get(MemorySegment from, MemorySegment to) {
        return ssTable.range(getInMemory(from, to), from, to);
    }

    public Iterator<Entry<MemorySegment>> getInMemory(MemorySegment from, MemorySegment to) {
        if (from == null && to == null) {
            return memory.values().iterator();
        } else if (from == null) {
            return memory.headMap(to, false).values().iterator();
        } else if (to == null) {
            return memory.tailMap(from, true).values().iterator();
        }

        return memory.subMap(from, true, to, false).values().iterator();
    }

    @Override
    public Entry<MemorySegment> get(MemorySegment key) {
        Entry<MemorySegment> entry = memory.get(key);

        if (entry == null) {
            entry = ssTable.readData(key);
        }

        if (entry != null && entry.value() == null) {
            return null;
        }

        return entry;
    }

    @Override
    public void upsert(Entry<MemorySegment> entry) {
        memory.put(entry.key(), entry);
    }

    @Override
    public Iterator<Entry<MemorySegment>> all() {
        return memory.values().iterator();
    }

    @Override
    public void compact() throws IOException {
        ssTable.compactData(path, () -> get(null, null));
        memory.clear();
    }

    @Override
    public void close() throws IOException {
        if (!arena.scope().isAlive()) {
            return;
        }

        arena.close();

        if (!memory.isEmpty()) {
            ssTable.saveMemData(path, memory.values());
        }
    }
}
