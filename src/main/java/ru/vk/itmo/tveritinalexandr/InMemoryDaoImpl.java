package ru.vk.itmo.tveritinalexandr;

import ru.vk.itmo.Config;
import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class InMemoryDaoImpl implements Dao<MemorySegment, Entry<MemorySegment>> {
    private final SortedMap<MemorySegment, Entry<MemorySegment>> dataBase =
            new ConcurrentSkipListMap<>(comparator);

    private static final MemorySegmentComparator comparator = MemorySegmentComparator.INSTANCE;
    private final SSTableSaver saver;
    private final SSTableLoader loader;

    public InMemoryDaoImpl(Config config) {
        Path storagePath = config.basePath().resolve("storage.sst");
        saver = new SSTableSaver(storagePath, dataBase);
        loader = new SSTableLoader(storagePath);
        loader.load();
    }

    @Override
    public Entry<MemorySegment> get(MemorySegment key) {
        var value = dataBase.get(key);
        return value == null ? loader.findInSSTable(key) : value;
    }

    @Override
    public Iterator<Entry<MemorySegment>> get(MemorySegment from, MemorySegment to) {
        if (from == null && to == null) {
            return dataBase.values().iterator();
        }
        if (from == null) {
            return dataBase.headMap(to).values().iterator();
        }
        if (to == null) {
            return dataBase.tailMap(from).values().iterator();
        }
        return dataBase.subMap(from, to).values().iterator();
    }

    @Override
    public void upsert(Entry<MemorySegment> entry) {
        if (entry == null) return;

        dataBase.put(entry.key(), entry);
    }

    @Override
    public void close() throws IOException {
        saver.save();
    }
}
