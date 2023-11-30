package ru.vk.itmo.supriadkinadaria;

import ru.vk.itmo.Config;
import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;


public class MemorySegmentDao implements Dao<MemorySegment, Entry<MemorySegment>> {

    private final Comparator<MemorySegment> comparator = MemorySegmentDao::compare;
    private final NavigableMap<MemorySegment, Entry<MemorySegment>> storage = new ConcurrentSkipListMap<>(comparator);
    private NavigableMap<MemorySegment, Entry<MemorySegment>> storage2 = new ConcurrentSkipListMap<>(comparator);
    private final Arena arena;
    private final DiskStorage diskStorage;
    private final Path path;
    private final long flushThresholdBytes;
    private FlushProcess flushProcess;
    private  CompactionProcess compactionProcess;

    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    private Future flushFuture;

    private Future compactionFuture;

    AtomicLong storageBytesSize = new AtomicLong(0L);
    private final ExecutorService executor = Executors.newSingleThreadExecutor();

    public MemorySegmentDao(Config config) throws IOException {
        this.path = config.basePath().resolve("data");
        this.flushThresholdBytes = config.flushThresholdBytes();
        Files.createDirectories(path);

        arena = Arena.ofShared();

        this.diskStorage = new DiskStorage(DiskStorage.loadOrRecover(path, arena));
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

    @Override
    public Iterator<Entry<MemorySegment>> get(MemorySegment from, MemorySegment to) {
        List<Iterator<Entry<MemorySegment>>> memory = new ArrayList<>(List.of(getInMemory(storage, from, to)));
        if (processInProgress(flushFuture)) {
            memory.add(getInMemory(storage2, from, to));
        }
        return diskStorage.range(from, to, memory);
    }

    private Iterator<Entry<MemorySegment>> getInMemory(NavigableMap<MemorySegment, Entry<MemorySegment>> storage, MemorySegment from, MemorySegment to) {
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

    @Override
    public void upsert(Entry<MemorySegment> entry) {
        lock.readLock().lock();
        try {
            if (storageBytesSize.get() >= flushThresholdBytes && processInProgress(flushFuture)) {
                throw new RuntimeException();
            }

            long size = entry.value() == null ? Long.BYTES : entry.value().byteSize();

            if (storage.put(entry.key(), entry) == null) {
                storageBytesSize.addAndGet(entry.key().byteSize() + size);
            } else {
                storageBytesSize.addAndGet(size);
            }
        } finally {
            lock.readLock().unlock();
        }
        try {
            if (storageBytesSize.get() >= flushThresholdBytes) {
                flush();
            }
        } catch(IOException e) {
            throw new RuntimeException();
        }


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
        Entry<MemorySegment> entry2 = storage2.get(key);
        if (entry2 != null) {
            if (entry2.value() == null) {
                return null;
            }
            return entry2;
        }

        Iterator<Entry<MemorySegment>> iterator = diskStorage.range(key, null, List.of(Collections.emptyIterator()));

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
    public synchronized void compact() throws IOException {
        if (!processInProgress(compactionFuture)) {
            lock.writeLock().lock();
            try {
                compactionProcess = new CompactionProcess(diskStorage, path, storage, arena, get(null, null));
                compactionFuture = executor.submit(compactionProcess);
            } finally {
                lock.writeLock().unlock();
            }
        }
    }

    @Override
    public void flush() throws IOException {
        if (!processInProgress(flushFuture) && !storage.isEmpty()) {
            lock.writeLock().lock();
            try {
                storage2 = storage;
                storageBytesSize.set(0);

                flushProcess = new FlushProcess(diskStorage, path, storage2, arena);
                flushFuture = executor.submit(flushProcess);
            } finally {
                clearStorages();
                lock.writeLock().unlock();
            }
        }
    }

    @Override
    public void close() throws IOException {
        try {
            waitForProcessEnd(flushFuture);
            waitForProcessEnd(compactionFuture);
        } catch (ExecutionException | InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        if (!storage.isEmpty()) {
            diskStorage.save(path, storage.values(), arena);
        }

        executor.shutdownNow();
        arena.close();
        storage.clear();
        storage2.clear();
    }

    private boolean processInProgress(Future process) {
        return process != null && !process.isDone();
    }

    private void waitForProcessEnd(Future process) throws ExecutionException, InterruptedException {
        if (processInProgress(process)) {
            process.get();
        }
    }

    private void clearStorages() {
        storage.clear();
        storage2.clear();
    }

    private static class FlushProcess implements Runnable {

        DiskStorage diskStorage;
        Path path;
        NavigableMap<MemorySegment, Entry<MemorySegment>> storage;
        Arena arena;
        public FlushProcess(DiskStorage diskStorage, Path path,
                            NavigableMap<MemorySegment, Entry<MemorySegment>> storage,
                            Arena arena) {
            this.diskStorage = diskStorage;
            this.storage = storage;
            this.path = path;
            this.arena = arena;
        }
        @Override
        public void run() {
            try {
                diskStorage.save(path, storage.values(), arena);
                storage.clear();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private static class CompactionProcess implements Runnable {
        DiskStorage diskStorage;
        Path path;
        NavigableMap<MemorySegment, Entry<MemorySegment>> storage;
        Arena arena;
        Iterator<Entry<MemorySegment>> iterator;
        public CompactionProcess(DiskStorage diskStorage, Path path,
                                 NavigableMap<MemorySegment, Entry<MemorySegment>> storage,
                                 Arena arena, Iterator<Entry<MemorySegment>> iterator) {
            this.diskStorage = diskStorage;
            this.storage = storage;
            this.path = path;
            this.arena = arena;
            this.iterator = iterator;
        }

        @Override
        public void run() {
            try {
                diskStorage.compact(path, () -> iterator);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            storage.clear();
        }
    }
}
