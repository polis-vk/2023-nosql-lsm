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
import java.util.Iterator;
import java.util.List;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

public class ChernyshevDao implements Dao<MemorySegment, Entry<MemorySegment>> {

    private static final String DATA_PATH = "data";
    //private final Comparator<MemorySegment> comparator = ChernyshevDao::compare;
    private final Arena arena;
    private final DiskStorage diskStorage;
    private final Path path;
    private final long flushThresholdBytes;
    private final ExecutorService executor = Executors.newSingleThreadExecutor(e ->
            new Thread(e, "Background thread"));
    private final List<Future<?>> backgroundResults = new CopyOnWriteArrayList<>();
    //private final ReadWriteLock lock = new ReentrantReadWriteLock();
    private final Memory memory = new Memory();
    private final AtomicBoolean isBackgroundFlushingInProgress = new AtomicBoolean(false);
    private final AtomicBoolean isClosed = new AtomicBoolean(false);

    private static class Memory {

        Memory() {

        }

        private long size = 0;

        private int isReserveStorage = 0;
        private final NavigableMap<MemorySegment, Entry<MemorySegment>> storageFirst =
                new ConcurrentSkipListMap<>(ChernyshevDao::compare);
        private final NavigableMap<MemorySegment, Entry<MemorySegment>> storageSecond =
                new ConcurrentSkipListMap<>(ChernyshevDao::compare);

        private NavigableMap<MemorySegment, Entry<MemorySegment>> getActualMemory() {
            if (isReserveStorage == 0) {
                return this.storageFirst;
            }
            else {
                return this.storageSecond;
            }
        }

        private NavigableMap<MemorySegment, Entry<MemorySegment>> getReserveMemory() {
            if (isReserveStorage == 1) {
                return this.storageFirst;
            }
            else {
                return this.storageSecond;
            }
        }

        private void put(Entry<MemorySegment> entry) {
            storageFirst.put(entry.key(), entry);
        }

        private void changeStorage() {
            isReserveStorage = (isReserveStorage + 1) % 2;
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
        if (isClosed.get()) {
            throw new RuntimeException("Dao is closed");
        }
        return diskStorage.range(getInActualMemory(from, to), getInReserveMemory(from, to), from, to);
    }

    private Iterator<Entry<MemorySegment>> getInActualMemory(MemorySegment from, MemorySegment to) {
        if (isClosed.get()) {
            throw new RuntimeException("Dao is closed");
        }
        if (from == null && to == null) {
            return memory.getActualMemory().values().iterator();
        }
        if (from == null) {
            return memory.getActualMemory().headMap(to).values().iterator();
        }
        if (to == null) {
            return memory.getActualMemory().tailMap(from).values().iterator();
        }
        return memory.getActualMemory().subMap(from, to).values().iterator();
    }

    private Iterator<Entry<MemorySegment>> getInReserveMemory(MemorySegment from, MemorySegment to) {
        if (isClosed.get()) {
            throw new RuntimeException("Dao is closed");
        }
        if (from == null && to == null) {
            return memory.getReserveMemory().values().iterator();
        }
        if (from == null) {
            return memory.getReserveMemory().headMap(to).values().iterator();
        }
        if (to == null) {
            return memory.getReserveMemory().tailMap(from).values().iterator();
        }
        return memory.getReserveMemory().subMap(from, to).values().iterator();
    }

    @Override
    public Entry<MemorySegment> get(MemorySegment key) {
        if (isClosed.get()) {
            throw new RuntimeException("Dao is closed");
        }
        Entry<MemorySegment> entry = memory.getActualMemory().get(key);
        if (entry != null) {
            if (entry.value() == null) {
                return null;
            }
            return entry;
        }
        else {
            entry = memory.getReserveMemory().get(key);
            if (entry != null) {
                if (entry.value() == null) {
                    return null;
                }
                return entry;
            }
        }

        Iterator<Entry<MemorySegment>> iterator = diskStorage.range(Collections.emptyIterator(), Collections.emptyIterator(), key, null);

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
    public synchronized void close() throws IOException {
        if (isClosed.get()) {
            return;
        }

        if (!arena.scope().isAlive()) {
            return;
        }

        arena.close();

        flush();

        executor.shutdown();
        for (Future<?> result : backgroundResults) {
            if (!result.isDone()) {
                try {
                    result.get();
                } catch (ExecutionException | InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        }
        backgroundResults.clear();
        executor.close();
        isClosed.set(true);
    }

    @Override
    public void flush() throws IOException {
        if (isClosed.get()) {
            throw new RuntimeException("Dao is closed");
        }
        if (!memory.getActualMemory().isEmpty() && !isBackgroundFlushingInProgress.get()) {
            isBackgroundFlushingInProgress.set(true);
            backgroundResults.add(executor.submit(() -> {
                FileUtils.save(path, memory.getActualMemory().values(), false);
                memory.getActualMemory().clear();
                isBackgroundFlushingInProgress.set(false);
            }));
        }
    }

    public void flushReserve() throws IOException {
        if (isClosed.get()) {
            throw new RuntimeException("Dao is closed");
        }
        if (!memory.getReserveMemory().isEmpty()) {
            backgroundResults.add(executor.submit(() -> {
                FileUtils.save(path, memory.getReserveMemory().values(), false);
                memory.getReserveMemory().clear();
                isBackgroundFlushingInProgress.set(false);
            }));
        }
    }

    @Override
    public void compact() throws IOException {
        if (isClosed.get()) {
            throw new RuntimeException("Dao is closed");
        }
        //flush();
        if (all().hasNext()) {
            backgroundResults.add(executor.submit(() -> FileUtils.compact(path, this::all)));
        }
    }

    @Override
    public void upsert(Entry<MemorySegment> entry) {
        if (isClosed.get()) {
            throw new RuntimeException("Dao is closed");
        }
        if (entry == null || entry.key() == null) {
            throw new RuntimeException("entry/key is null");
        }
        long entrySize = entry.key().byteSize() * 2 + (entry.value() != null ? entry.value().byteSize() : 0);
        synchronized (this) {
            if (memory.size + entrySize > flushThresholdBytes) {
                if (isBackgroundFlushingInProgress.getAndSet(true)) {
                    throw new RuntimeException("flushThresholdBytes reached; Automatic flush is in progress");
                } else {
                    //isBackgroundFlushingInProgress.set(true);
                    try {
                        memory.changeStorage();
                        memory.size = 0;
                                flushReserve();

                            } catch (IOException e) {
                                throw new RuntimeException(e);
                    }
                }
            }
            else {
                memory.size += entrySize;
            }
        }
        memory.put(entry);
    }
}
