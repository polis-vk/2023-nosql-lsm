package ru.vk.itmo.chernyshevyaroslav;

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
//import java.util.Comparator;
import java.util.Iterator;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicBoolean;

public class ChernyshevDao implements Dao<MemorySegment, Entry<MemorySegment>> {

    private static final String DATA_PATH = "data";
    //private final Comparator<MemorySegment> comparator = ChernyshevDao::compare;
    private final Arena arena;
    private final DiskStorage diskStorage;
    private final Path path;
    private final long flushThresholdBytes;
    private long size = 0;
    private final AtomicBoolean isThresholdReached = new AtomicBoolean(false);

    private static class Memory {
        private static final NavigableMap<MemorySegment, Entry<MemorySegment>> storage =
                new ConcurrentSkipListMap<>(ChernyshevDao::compare);
        //private static final NavigableMap<MemorySegment, Entry<MemorySegment>> storageDouble =
        //        new ConcurrentSkipListMap<>(ChernyshevDao::compare);

        private static NavigableMap<MemorySegment, Entry<MemorySegment>> getStorage() {
            return storage;
        }
    }

    public ChernyshevDao(Config config) throws IOException {
        this.path = config.basePath().resolve(DATA_PATH);
        this.flushThresholdBytes = config.flushThresholdBytes();
        Files.createDirectories(path);

        arena = Arena.ofShared();

        this.diskStorage = new DiskStorage(FileUtils.loadOrRecover(path, arena));
    }

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
        return Byte.compareUnsigned(b1, b2);
    }

    @Override
    public Iterator<Entry<MemorySegment>> get(MemorySegment from, MemorySegment to) {
        return diskStorage.range(getInMemory(from, to), from, to);
    }

    private Iterator<Entry<MemorySegment>> getInMemory(MemorySegment from, MemorySegment to) {
        if (from == null && to == null) {
            return Memory.getStorage().values().iterator();
        }
        if (from == null) {
            return Memory.getStorage().headMap(to).values().iterator();
        }
        if (to == null) {
            return Memory.getStorage().tailMap(from).values().iterator();
        }
        return Memory.getStorage().subMap(from, to).values().iterator();
    }

    @Override
    public Entry<MemorySegment> get(MemorySegment key) {
        Entry<MemorySegment> entry = Memory.getStorage().get(key);
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
    public void close() throws IOException {
        if (!arena.scope().isAlive()) {
            return;
        }

        arena.close();

        flush();
    }

    @Override
    public synchronized void flush() throws IOException {
        if (!Memory.getStorage().isEmpty()) {
            FileUtils.save(path, Memory.getStorage().values(), false);
        }
    }

    @Override
    public void compact() throws IOException {
        //flush();
        if (all().hasNext()) {
            FileUtils.compact(path, this::all);
        }
    }

    @Override
    public void upsert(Entry<MemorySegment> entry) {
        long entrySize = entry.key().byteSize() * 2 + (entry.value() != null ? entry.value().byteSize() : 0);
        if (size + entrySize > flushThresholdBytes) {
            if (isThresholdReached.get()) {
                throw new RuntimeException("flushThresholdBytes reached; Automatic flush is in progress");
            } else {
                isThresholdReached.set(true);
                size += entrySize;
                try {
                    new Thread(() -> {
                        try {
                            flush();
                            Memory.getStorage().clear();
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    }).start();
                } finally {
                    isThresholdReached.set(false);
                }
            }
        }

        Memory.storage.put(entry.key(), entry);
    }
}
