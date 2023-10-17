package ru.vk.itmo.kovalevigor;

import ru.vk.itmo.Config;
import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.Objects;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class DaoImpl implements Dao<MemorySegment, Entry<MemorySegment>> {

    private final Arena memoryArena;
    private final SSTable ssTable;
    private final ConcurrentNavigableMap<MemorySegment, Entry<MemorySegment>> storage;
    public static final String SSTABLE_NAME = "sstable";
    public static final long TABLE_SIZE_LIMIT = 1024 * 8; // TODO: От балды

    public DaoImpl(final Config config) throws IOException {
        memoryArena = Arena.ofShared();
        ssTable = new SSTable(getSSTablePath(config.basePath()));
        storage = new ConcurrentSkipListMap<>(ssTable.load(memoryArena, TABLE_SIZE_LIMIT));
    }

    private static <T> Iterator<T> getValuesIterator(final ConcurrentNavigableMap<?, T> map) {
        return map.values().iterator();
    }

    @Override
    public Iterator<Entry<MemorySegment>> get(final MemorySegment from, final MemorySegment to) {
        if (from == null) {
            if (to == null) {
                return all();
            }
            return allTo(to);
        } else if (to == null) {
            return allFrom(from);
        }
        return getValuesIterator(storage.subMap(from, to));
    }

    @Override
    public void upsert(final Entry<MemorySegment> entry) {
        Objects.requireNonNull(entry);
        storage.put(entry.key(), entry);
    }

    @Override
    public Iterator<Entry<MemorySegment>> allFrom(final MemorySegment from) {
        Objects.requireNonNull(from);
        return getValuesIterator(storage.tailMap(from));
    }

    @Override
    public Iterator<Entry<MemorySegment>> allTo(final MemorySegment to) {
        Objects.requireNonNull(to);
        return getValuesIterator(storage.headMap(to));
    }

    @Override
    public Iterator<Entry<MemorySegment>> all() {
        return getValuesIterator(storage);
    }

    @Override
    public Entry<MemorySegment> get(final MemorySegment key) {
        Objects.requireNonNull(key);
        final Entry<MemorySegment> result = storage.get(key);
        if (result != null) {
            return result;
        }
        try {
            return ssTable.get(key, memoryArena);
        } catch (IOException e) {
            return null;
        }
    }

    private Path getSSTablePath(final Path base) {
        return base.resolve(SSTABLE_NAME);
    }

    @Override
    public void close() throws IOException {
        if (!memoryArena.scope().isAlive()) {
            return;
        }
        ssTable.write(storage);
        memoryArena.close();
        storage.clear();
    }
}
