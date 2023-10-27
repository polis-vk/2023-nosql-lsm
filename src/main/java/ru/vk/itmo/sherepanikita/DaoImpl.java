package ru.vk.itmo.sherepanikita;

import ru.vk.itmo.Config;
import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.util.Iterator;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class DaoImpl implements Dao<MemorySegment, Entry<MemorySegment>> {

    private final NavigableMap<MemorySegment, Entry<MemorySegment>> segments;

    public DaoImpl() {
        segments = new ConcurrentSkipListMap<>(new MemorySegmentComparator());
        ssTable = createSSTableOrNull(new Config(null));
    }

    public DaoImpl(Config config) {
        segments = new ConcurrentSkipListMap<>(new MemorySegmentComparator());
        ssTable = createSSTableOrNull(config);
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
            save(path, storage.values());
        }
    }


    @Override
    public Iterator<Entry<MemorySegment>> iterator() {
        return all();
    }

    @Override
    public void compact() throws IOException {
        Path compressedStoragePath = Path.of("data_compacted");
        Files.createDirectories(compressedStoragePath);
        DiskStorage.save(compressedStoragePath, this);
        DiskStorage.deleteOldStorage(path);

        path = compressedStoragePath;
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
