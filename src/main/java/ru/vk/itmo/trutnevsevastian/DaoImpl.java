package ru.vk.itmo.trutnevsevastian;

import ru.vk.itmo.BaseEntry;
import ru.vk.itmo.Config;
import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Map;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class DaoImpl implements Dao<MemorySegment, Entry<MemorySegment>> {

    private static String SS_TABLE_FILE_NAME = "table.ex";

    private final Comparator<MemorySegment> comparator = new MemorySegmentComparator();
    private final NavigableMap<MemorySegment, Entry<MemorySegment>> storage = new ConcurrentSkipListMap<>(comparator);

    private final SSTableRepository repository = new SSTableRepository(comparator);

    private final SSTable lastSSTable;

    private final Path path;

    public DaoImpl(Config config) throws IOException {
        path = config.basePath();
        var filePath = path.resolve(SS_TABLE_FILE_NAME);
        if (Files.exists(filePath)) {
            lastSSTable = repository.read(filePath);
        } else {
            lastSSTable = null;
        }
    }

    @Override
    public Iterator<Entry<MemorySegment>> get(MemorySegment from, MemorySegment to) {
        return subMap(from, to).values().iterator();
    }

    private Map<MemorySegment, Entry<MemorySegment>> subMap(MemorySegment from, MemorySegment to) {
        if (from == null && to == null) {
            return storage;
        }
        if (from == null) {
            return storage.headMap(to);
        }
        if (to == null) {
            return storage.tailMap(from, true);
        }

        return storage.subMap(from, true, to, false);
    }

    @Override
    public void upsert(Entry<MemorySegment> entry) {
        storage.put(entry.key(), entry);
    }

    @Override
    public void flush() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Entry<MemorySegment> get(MemorySegment key) {
        var entryInMemTable = storage.get(key);
        if (entryInMemTable != null || lastSSTable == null) return entryInMemTable;
        var value = lastSSTable.get(key);
        if (value == null) return null;
        return new BaseEntry<>(key, value);
    }

    @Override
    public void close() throws IOException {
        repository.save(storage, path.resolve(SS_TABLE_FILE_NAME));
    }
}
