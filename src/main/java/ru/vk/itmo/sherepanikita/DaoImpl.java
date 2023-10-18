package ru.vk.itmo.sherepanikita;

import ru.vk.itmo.Config;
import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.stream.Stream;

public class DaoImpl implements Dao<MemorySegment, Entry<MemorySegment>> {

    private final NavigableMap<MemorySegment, Entry<MemorySegment>> segments;

    private final List<Storage> storages;

    public DaoImpl() {
        segments = new ConcurrentSkipListMap<>(new MemorySegmentComparator());
        storages = new ArrayList<>();
    }

    public DaoImpl(Config config) {
        segments = new ConcurrentSkipListMap<>(new MemorySegmentComparator());
        storages = new ArrayList<>();

        List<Path> storagesPaths = getAllStoragesPathsFromDirectory(config.basePath());

        for (Path storagePath : storagesPaths) {
            storages.add(createStorageOrNull(new Config(storagePath)));
        }
    }

    @Override
    public Iterator<Entry<MemorySegment>> get(MemorySegment from, MemorySegment to) {
        List<PeekIterator> iterators = new ArrayList<>();
        iterators.add(new PeekIterator(getFromMemory(from, to)));
        for (Storage storage : storages) {
            iterators.add(new PeekIterator(storage.getIterator(from, to)));
        }
        return new MergeIterator(iterators);
    }

    public Iterator<Entry<MemorySegment>> getFromMemory(MemorySegment from, MemorySegment to) {
        if (from == null && to == null) {
            return segments.values().iterator();
        } else if (from == null) {
            return segments.headMap(to).values().iterator();
        } else if (to == null) {
            return segments.tailMap(from).values().iterator();
        }
        return segments.subMap(from, to).values().iterator();
    }


    @Override
    public Entry<MemorySegment> get(MemorySegment key) {
        Iterator<Entry<MemorySegment>> iterator = get(key, null);
        var currentEntry = iterator.next();
        if (key.mismatch(currentEntry.key()) == -1) {
            return currentEntry;
        }
        return null;
    }

    @Override
    public void upsert(Entry<MemorySegment> entry) {
        if (entry == null) {
            throw new IllegalArgumentException("Incoming entry is NULL");
        }
        segments.put(entry.key(), entry);
    }

    private Storage createStorageOrNull(Config config) {
        if (config.basePath() == null) {
            return null;
        }
        try {
            return new Storage(config);
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }

    private List<Path> getAllStoragesPathsFromDirectory(Path directory) {
        List<Path> storagesPaths = new ArrayList<>();
        try (Stream<Path> paths = Files.walk(directory)) {
            paths.forEach(storagesPaths::add);
        } catch (IOException ex) {
            ex.printStackTrace();
            return null;
        }
        return storagesPaths;
    }
}
