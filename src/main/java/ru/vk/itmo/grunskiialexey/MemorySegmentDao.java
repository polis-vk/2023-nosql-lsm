package ru.vk.itmo.grunskiialexey;

import ru.vk.itmo.Config;
import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;

/*
5 minutes for describing class
class which gives to client main things like

1. Make and check files for having data
2. get(key) - may be better
3. get(range) - okey launch storage
4. flush - will be in different FlushClass
5. compact - will be in different CompactClass
 */
public class MemorySegmentDao implements Dao<MemorySegment, Entry<MemorySegment>> {
    // no necessary
    private final Comparator<MemorySegment> comparator = MemorySegmentDao::compare;
    private final NavigableMap<MemorySegment, Entry<MemorySegment>> storage = new ConcurrentSkipListMap<>(comparator);
    private final NavigableMap<MemorySegment, Entry<MemorySegment>> storage2 = new ConcurrentSkipListMap<>(comparator);
    private final Arena arena;
    private final CompactionService compactionService;
    private final FlushService flushService;
    private final Path path;

    public MemorySegmentDao(Config config) throws IOException {
        this.path = config.basePath().resolve("data");
        Files.createDirectories(path);

        arena = Arena.ofShared();

        final AtomicLong lastFileNumber = new AtomicLong();
        this.compactionService = new CompactionService(DiskStorage.loadOrRecover(path, arena, lastFileNumber), lastFileNumber);
        this.flushService = new FlushService(path, config.flushThresholdBytes(), lastFileNumber);
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
        return Byte.compare(b1, b2);
    }

    // may get more better query
    @Override
    public Iterator<Entry<MemorySegment>> get(MemorySegment from, MemorySegment to) {
        return compactionService.range(getInMemoryIterators(from, to), from, to);
    }

    private List<Iterator<Entry<MemorySegment>>> getInMemoryIterators(MemorySegment from, MemorySegment to) {
        if (from == null && to == null) {
            return List.of(storage.values().iterator(), storage2.values().iterator());
        }
        if (from == null) {
            return List.of(
                    storage.headMap(to).values().iterator(),
                    storage2.headMap(to).values().iterator()
            );
        }
        if (to == null) {
            return List.of(
                    storage.tailMap(from).values().iterator(),
                    storage2.tailMap(from).values().iterator()
            );
        }
        return List.of(
                storage.subMap(from, to).values().iterator(),
                storage2.subMap(from, to).values().iterator()
        );
    }

    @Override
    public void upsert(Entry<MemorySegment> entry) {
//        if (flush.isReachedThreshold(entry) && flush.isWorking().get()) {
//            try {
//                Thread.sleep(10);
//            } catch (InterruptedException e) {
//                throw new RuntimeException(e);
//            }
//        }
//        if (flush.isReachedThreshold(entry) && flush.isWorking().get()) {
//            throw new OutOfMemoryError("Can't upsert data in flushing file");
//        }
//
//        if (flush.isReachedThreshold(entry)) {
//            try {
//                flush();
//            } catch (IOException e) {
//                throw new RuntimeException(e);
//            }
//        }
//
//        if (flush.isWorking().get() && isUpserting.compareAndSet(false, true)) {
//            flushStorage.put(entry.key(), entry);
//            isUpserting.set(false);
//        }
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

        Iterator<Entry<MemorySegment>> iterator = compactionService.range(key, null);

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
    public void flush() throws IOException {
        if (!storage.isEmpty()) {
            flushService.save(storage.values());
        }
    }

    @Override
    public void compact() throws IOException {
        compactionService.compact(path);
    }

    @Override
    public void close() throws IOException {
        if (!arena.scope().isAlive()) {
            return;
        }

        flush();

        arena.close();
    }
}
