package ru.vk.itmo.smirnovandrew;

import ru.vk.itmo.Config;
import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class MemorySegmentDao implements Dao<MemorySegment, Entry<MemorySegment>>, Iterable<Entry<MemorySegment>> {

    private Path tablePath;

    private final Config config;

    private DiskStorage diskStorage;

    private final Arena arena;

    private int idx;

    private final NavigableMap<MemorySegment, Entry<MemorySegment>> segments =
            new ConcurrentSkipListMap<>(segmentComparator);

    private static final Comparator<MemorySegment> segmentComparator = MemorySegmentDao::compare;

    static int compare(MemorySegment memorySegment1, MemorySegment memorySegment2) {
        long mismatch = memorySegment1.mismatch(memorySegment2);
        if (mismatch == -1) {
            return 0;
        }

        if (mismatch == memorySegment1.byteSize()) {
            return -1;
        }

        if (mismatch == memorySegment2.byteSize()) {
            return 1;
        }
        byte b1 = memorySegment1.get(ValueLayout.JAVA_BYTE, mismatch);
        byte b2 = memorySegment2.get(ValueLayout.JAVA_BYTE, mismatch);
        return Byte.compare(b1, b2);
    }

    private Path getPathByIdx(int idx) {
        return config.basePath().resolve(String.format("data%d", idx));
    }

    public MemorySegmentDao(Config config) throws IOException {
        this.config = config;

        arena = Arena.ofShared();

        idx = 0;
        this.tablePath = getPathByIdx(idx);
        Files.createDirectories(tablePath);

        if (!DiskStorage.checkIfExists(tablePath)) {
            idx = 1;
            this.tablePath = getPathByIdx(idx);
            Files.createDirectories(tablePath);
        }
        this.diskStorage = new DiskStorage(DiskStorage.loadOrRecover(tablePath, arena));
    }

    public MemorySegmentDao() {
        tablePath = null;
        arena = null;
        diskStorage = null;
        config = null;
    }

    private Iterator<Entry<MemorySegment>> getInMemory(MemorySegment from, MemorySegment to) {
        if (from == null && to == null) {
            return segments.values().iterator();
        }
        if (from == null) {
            return segments.headMap(to, false).values().iterator();
        }
        if (to == null) {
            return segments.tailMap(from, true).values().iterator();
        }

        return segments.subMap(from, true, to, false).values().iterator();
    }

    @Override
    public Iterator<Entry<MemorySegment>> get(MemorySegment from, MemorySegment to) {
        return diskStorage.range(getInMemory(from, to), from, to);
    }

    @Override
    public Entry<MemorySegment> get(MemorySegment key) {
        Entry<MemorySegment> entry = segments.get(key);
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
        if (compare(next.key(), key) == 0) {
            return next;
        }
        return null;
    }

    @Override
    public void upsert(Entry<MemorySegment> entry) {
        segments.put(entry.key(), entry);
    }

    @Override
    public void flush() throws IOException {
        if (!segments.isEmpty()) {
            DiskStorageHelper.save(tablePath, segments.values());
        }
    }

    @Override
    public void close() throws IOException {
        if (!arena.scope().isAlive()) {
            return;
        }

        arena.close();

        flush();
    }

    @Override
    public void compact() throws IOException {
        flush();

        int newIdx = (idx + 1) % 2;
        Path newPath = getPathByIdx(newIdx);
        Files.createDirectories(newPath);
        DiskStorageHelper.save(newPath, this);
        DiskStorage.removeTables(tablePath);

        idx = newIdx;
        tablePath = newPath;
        diskStorage = new DiskStorage(DiskStorage.loadOrRecover(tablePath, arena));
    }

    @Override
    public Iterator<Entry<MemorySegment>> iterator() {
        return this.all();
    }
}
