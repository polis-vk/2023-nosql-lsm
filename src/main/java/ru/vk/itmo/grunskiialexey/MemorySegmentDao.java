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
import java.util.Iterator;
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
    private final Arena arena;
    private final CompactionService compactionService;
    private final InMemoryQuerySystem inMemoryQuerySystem;
    private final Path path;

    public MemorySegmentDao(Config config) throws IOException {
        this.path = config.basePath().resolve("data");
        Files.createDirectories(path);

        arena = Arena.ofShared();

        final AtomicLong firstFileNumber = new AtomicLong();
        final AtomicLong lastFileNumber = new AtomicLong();
        DiskStorage diskStorage = new DiskStorage(path, arena);
        this.compactionService = new CompactionService(
                diskStorage.loadOrRecover(firstFileNumber, lastFileNumber),
                firstFileNumber,
                lastFileNumber,
                arena
        );
        this.inMemoryQuerySystem = new InMemoryQuerySystem(
                path,
                config.flushThresholdBytes(),
                MemorySegmentDao::compare,
                lastFileNumber,
                diskStorage,
                arena
        );
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
        return compactionService.range(inMemoryQuerySystem.getInMemoryIterators(from, to), from, to);
    }

    @Override
    public void upsert(Entry<MemorySegment> entry) {
        inMemoryQuerySystem.upsert(entry);
    }

    @Override
    public Entry<MemorySegment> get(MemorySegment key) {
        Entry<MemorySegment> entry = inMemoryQuerySystem.get(key);
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
        inMemoryQuerySystem.flush();
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
