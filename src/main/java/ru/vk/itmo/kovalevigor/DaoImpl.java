package ru.vk.itmo.kovalevigor;

import ru.vk.itmo.Config;
import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;

import java.io.File;
import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
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
    public final Path path;

    public DaoImpl(final Config config) throws IOException {
        path = config.basePath();
        memoryArena = Arena.ofShared();
        storage = new ConcurrentSkipListMap<>(SSTable.COMPARATOR);
        if (Files.notExists(path.resolve(SSTABLE_NAME))) {
            ssTable = null;
        } else {
            ssTable = new SSTable(path, SSTABLE_NAME, memoryArena);
        }

//        storage = new ConcurrentSkipListMap<>(ssTable.load(memoryArena, TABLE_SIZE_LIMIT));
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
        if (ssTable == null) {
            return null;
        }
        try {
            return ssTable.get(key);
        } catch (IOException e) {
            return null;
        }
    }

    @Override
    public void close() throws IOException {
        if (!memoryArena.scope().isAlive()) {
            return;
        }
        if (!storage.isEmpty()) {
            SSTable.write(storage, path, SSTABLE_NAME);
        }
        memoryArena.close();
        storage.clear();
    }
}
