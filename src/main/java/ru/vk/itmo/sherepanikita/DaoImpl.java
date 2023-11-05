package ru.vk.itmo.sherepanikita;

import ru.vk.itmo.Config;
import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class DaoImpl implements Dao<MemorySegment, Entry<MemorySegment>>, Iterable<Entry<MemorySegment>> {

    private final Comparator<MemorySegment> comparator = new MemorySegmentComparator();
    private final NavigableMap<MemorySegment, Entry<MemorySegment>> storage = new ConcurrentSkipListMap<>(comparator);
    private final Arena arena;
    private DiskStorage diskStorage;
    private Path path;
    private Config config;
    private int fileIndex;
    private static final String FILE_NAME = "data%d";

    public DaoImpl() throws IOException {
        arena = Arena.ofShared();
        this.path = Path.of("");
        this.diskStorage = new DiskStorage(DiskStorage.loadOrRecover(path, arena));
    }

    public DaoImpl(Config config) throws IOException {
        this.config = config;
        this.fileIndex = 0;
        this.path = this.config.basePath().resolve(String.format(FILE_NAME, fileIndex));
        Files.createDirectories(path);

        Path indexTmp = Utils.getIndexTmp(path);
        Path indexFile = Utils.getIndexFile(path);
        if (!(Files.exists(indexTmp) || Files.exists(indexFile))) {
            fileIndex = 1;
            this.path = this.config.basePath().resolve(String.format(FILE_NAME, fileIndex));
            Files.createDirectories(path);
        }

        arena = Arena.ofShared();
        this.diskStorage = new DiskStorage(DiskStorage.loadOrRecover(path, arena));
    }

    @Override
    public Iterator<Entry<MemorySegment>> get(MemorySegment from, MemorySegment to) {
        return diskStorage.range(getInMemory(from, to), from, to);
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
        if (comparator.compare(next.key(), key) == 0) {
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

    @Override
    public Iterator<Entry<MemorySegment>> iterator() {
        return all();
    }

    @Override
    public void compact() throws IOException {
        if (!storage.isEmpty()) {
            DiskStorage.save(path, storage.values());
        }

        if (fileIndex == 1) {
            fileIndex = 0;
        } else {
            fileIndex = 1;
        }
        Path compactedStoragePath = config.basePath().resolve(String.format(FILE_NAME, fileIndex));
        Files.createDirectories(compactedStoragePath);
        DiskStorage.save(compactedStoragePath, this);
        DiskStorage.deleteOldStorage(path);

        path = compactedStoragePath;
        diskStorage = new DiskStorage(DiskStorage.loadOrRecover(path, arena));
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
}
