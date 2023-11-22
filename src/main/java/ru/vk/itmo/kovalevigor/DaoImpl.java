package ru.vk.itmo.kovalevigor;

import ru.vk.itmo.Config;
import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Logger;

public class DaoImpl implements Dao<MemorySegment, Entry<MemorySegment>> {

    private final SSTableManager ssManager;
    private final static ConcurrentNavigableMap<MemorySegment, Entry<MemorySegment>> EMPTY_MAP =
            new ConcurrentSkipListMap<>(SSTable.COMPARATOR);
    private final AtomicReference<ConcurrentNavigableMap<MemorySegment, Entry<MemorySegment>>> flushedStorage;
    private final AtomicLong currentMemoryByteSize;
    private final long flushThresholdBytes;
    private final AtomicReference<ConcurrentNavigableMap<MemorySegment, Entry<MemorySegment>>> currentStorage;
    private final ExecutorService flushService;
    private final ExecutorService compactService;
    private Future<Void> flushFuture = null;

    public DaoImpl(final Config config) throws IOException {
        ssManager = new SSTableManager(config.basePath());
        currentStorage = new AtomicReference<>(new ConcurrentSkipListMap<>(SSTable.COMPARATOR));
        flushedStorage = new AtomicReference<>(EMPTY_MAP);
        flushThresholdBytes = config.flushThresholdBytes();
        currentMemoryByteSize = new AtomicLong();
        flushService = Executors.newSingleThreadExecutor();
        compactService = Executors.newSingleThreadExecutor();
    }

    private static <T> Iterator<T> getValuesIterator(final SortedMap<?, T> map) {
        return map.values().iterator();
    }

    private static Iterator<Entry<MemorySegment>> getIterator(
            final SortedMap<MemorySegment, Entry<MemorySegment>> sortedMap,
            final MemorySegment from,
            final MemorySegment to
    ) {
        if (from == null) {
            if (to == null) {
                return getValuesIterator(sortedMap);
            } else {
                return getValuesIterator(sortedMap.headMap(to));
            }
        } else if (to == null) {
            return getValuesIterator(sortedMap.tailMap(from));
        } else {
            return getValuesIterator(sortedMap.subMap(from, to));
        }
    }

    @Override
    public Iterator<Entry<MemorySegment>> get(final MemorySegment from, final MemorySegment to) {
        final List<PriorityShiftedIterator<Entry<MemorySegment>>> iterators = new ArrayList<>(3);
        iterators.add(new MemEntryPriorityIterator(0, getIterator(currentStorage.get(), from, to)));
        iterators.add(new MemEntryPriorityIterator(1, getIterator(flushedStorage.get(), from, to)));
        try {
            iterators.add(new MemEntryPriorityIterator(2, ssManager.get(from, to)));
        } catch (IOException e) {
            log(e);
        }
        return new MergeEntryIterator(iterators);
    }

    private static long getMemorySegmentSize(final MemorySegment memorySegment) {
        return memorySegment == null ? 0 : memorySegment.byteSize();
    }

    private static long getEntrySize(final Entry<MemorySegment> entry) {
        return getMemorySegmentSize(entry.key()) + getMemorySegmentSize(entry.value());
    }

    @Override
    public synchronized void upsert(final Entry<MemorySegment> entry) {
        Objects.requireNonNull(entry);
        final long entrySize = getEntrySize(entry);
        // Падает CompactionTest::overwrite
//        if (entrySize >= flushThresholdBytes) {
//            throw new IllegalArgumentException("Too large entry");
//        }
        currentStorage.get().put(entry.key(), entry);
        long lastValue = currentMemoryByteSize.get();
        while (true) {
            final long newSize = lastValue + entrySize;
            if (newSize >= flushThresholdBytes) {
                if (flushFuture != null && !flushFuture.isDone()) {
                    throw new IllegalStateException("Limit is reached. U should wait");
                }
                flush();
                return;
            } else if (currentMemoryByteSize.compareAndSet(lastValue, newSize)) {
                break;
            }
            lastValue = currentMemoryByteSize.get();
        }
    }

    @Override
    public Entry<MemorySegment> get(final MemorySegment key) {
        Objects.requireNonNull(key);
        Entry<MemorySegment> result = currentStorage.get().get(key);
        if (result == null) {
            result = flushedStorage.get().get(key);
        }
        if (result != null) {
            if (result.value() == null) {
                return null;
            }
            return result;
        }

        try {
            return ssManager.get(key);
        } catch (IOException e) {
            return null;
        }
    }

    @Override
    public synchronized void flush() {
        ConcurrentNavigableMap<MemorySegment, Entry<MemorySegment>> map = currentStorage.get();
        if (flushedStorage.compareAndSet(EMPTY_MAP, map)) {
            do {
                long lastValue = currentMemoryByteSize.get();
                currentStorage.set(new ConcurrentSkipListMap<>(SSTable.COMPARATOR));
                if (currentMemoryByteSize.compareAndSet(lastValue, 0)) {
                    flushFuture = flushService.submit(() -> {
                        try {
                            ssManager.write(map);
                        } catch (IOException e) {
                            Logger.getAnonymousLogger().log(Logger.getAnonymousLogger().getLevel(), e.getMessage());
                        } finally {
                            flushedStorage.set(EMPTY_MAP);
                        }
                    }, null);
                    return;
                }
            } while (true);
        }
    }

    private static void awaitShutdown(ExecutorService service) {
        while (true) {
            try {
                if (service.awaitTermination(1, TimeUnit.SECONDS)) {
                    return;
                }
            } catch (InterruptedException e) {
                log(e);
            }
        }
    }

    @Override
    public void compact() throws IOException {
        compactService.execute(() -> {
            try {
                ssManager.compact();
            } catch (IOException e) {
                log(e);
            }
        });
    }

    @Override
    public void close() throws IOException {
        try {
        if (flushFuture != null) {
            flushFuture.get();
        }
        } catch (InterruptedException | ExecutionException e) {
            log(e);
        }
        flush();
        compactService.shutdown();
        flushService.shutdown();
        awaitShutdown(compactService);
        awaitShutdown(flushService);
        currentStorage.get().clear();
        flushedStorage.get().clear();
        ssManager.close();
    }

    private static void log(Exception e) {
        Logger.getAnonymousLogger().log(Logger.getAnonymousLogger().getLevel(), e.getMessage());
    }
}
