package ru.vk.itmo.abramovilya;

import ru.vk.itmo.Config;
import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.Iterator;
import java.util.NavigableMap;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class DaoImpl implements Dao<MemorySegment, Entry<MemorySegment>> {
    private ConcurrentMapWithSize map =
            new ConcurrentMapWithSize(DaoImpl::compareMemorySegments);
    private ConcurrentMapWithSize flushingMap = null;
    private final Arena arena = Arena.ofShared();
    private final Storage storage;
    private final ReentrantLock flushLock  = new ReentrantLock();
    private final long flushThresholdBytes;
    private final ReadWriteLock mapUpsertExchangeLock = new ReentrantReadWriteLock();
    private final AtomicBoolean flushing = new AtomicBoolean(false);
    private final ExecutorService flushQueue = Executors.newSingleThreadExecutor();


    public DaoImpl(Config config) throws IOException {
        flushThresholdBytes = config.flushThresholdBytes();
        storage = new Storage(config, arena);
    }

    @Override
    public Iterator<Entry<MemorySegment>> get(MemorySegment from, MemorySegment to) {
        return new DaoIterator(storage.getTotalSStables(), from, to, storage, map, flushingMap);
    }

    @Override
    public Entry<MemorySegment> get(MemorySegment key) {
        var value = map.get(key);
        if (value != null) {
            if (value.value() != null) {
                return value;
            }
            return null;
        }
        if (flushingMap  != null) {
            var flushingValue = flushingMap.get(key);
            if (flushingValue != null) {
                if (flushingValue.value() != null) {
                    return flushingValue;
                }
                return null;
            }
        }
        return storage.get(key);
    }

    @Override
    public void upsert(Entry<MemorySegment> entry) {
        mapUpsertExchangeLock.readLock().lock();
        try {
            map.put(entry.key(), entry);
        } finally {
            mapUpsertExchangeLock.readLock().unlock();
        }

        if (map.memorySize() <= flushThresholdBytes) {
            return;
        }
        mapUpsertExchangeLock.writeLock().lock();
        try {
            if (map.memorySize() <= flushThresholdBytes) {
                return;
            }
            if (flushing.compareAndSet(false, true)) {
            flushingMap = map;
            map = new ConcurrentMapWithSize(DaoImpl::compareMemorySegments);
                flushQueue.execute(
                        () -> {
                            try {
                                flush(flushingMap);
                                flushingMap = null;
                            } catch (IOException e) {
                                throw new UncheckedIOException(e);
                            } finally {
                                flushing.set(false);
                            }
                        }
                );
            } else {
                // TODO: Заменить на DAOMemoryException
                throw new OutOfMemoryError("Upsert happened with no free space and flushing already executing");
            }
        } finally {
            mapUpsertExchangeLock.writeLock().unlock();
        }
    }

    @Override
    public void compact() throws IOException {
        var iterator = get(null, null);
        if (!iterator.hasNext()) {
            return;
        }
        storage.compact(iterator, get(null, null));
        map.clear();
    }

    // Одновременно может работать только один flush
    // Если какой-то flush в процессе исполнение и приходит запрос на еще один flush,
    // мы добавляем эту задачу в очередь и ждем завершения
    //
    // Вызов flush блокирует вызывающий его поток
    @Override
    public void flush() throws IOException {
        flush(map);
    }

    private void flush(NavigableMap<MemorySegment, Entry<MemorySegment>> navigableMap) throws IOException {
        flushLock.lock();
        flushing.set(true);
        try {
            writeMapIntoFile(navigableMap);
            storage.incTotalSStablesAmount();
        } finally {
            flushing.set(false);
            flushLock.unlock();
        }
    }

    private void writeMapIntoFile(NavigableMap<MemorySegment, Entry<MemorySegment>> mapToWrite) throws IOException {
        if (mapToWrite.isEmpty()) {
            return;
        }
        storage.writeMapIntoFile(
                mapByteSizeInFile(mapToWrite),
                indexByteSizeInFile(mapToWrite),
                mapToWrite
        );
    }

    private long mapByteSizeInFile(NavigableMap<MemorySegment, Entry<MemorySegment>> mapToWrite) {
        long size = 0;
        for (var entry : mapToWrite.values()) {
            size += 2 * Long.BYTES;
            size += entry.key().byteSize();
            if (entry.value() != null) {
                size += entry.value().byteSize();
            }
        }
        return size;
    }

    private long indexByteSizeInFile(NavigableMap<MemorySegment, Entry<MemorySegment>> mapToWrite) {
        return (long) mapToWrite.size() * (Integer.BYTES + Long.BYTES);
    }

    @Override
    public void close() throws IOException {
        if (!map.isEmpty()) {
            flush();
        }
        if (arena.scope().isAlive()) {
            arena.close();
        }
        storage.close();
    }

    public static int compareMemorySegments(MemorySegment segment1, MemorySegment segment2) {
        long offset = segment1.mismatch(segment2);
        if (offset == -1) {
            return 0;
        } else if (offset == segment1.byteSize()) {
            return -1;
        } else if (offset == segment2.byteSize()) {
            return 1;
        }
        return Byte.compare(segment1.get(ValueLayout.JAVA_BYTE, offset), segment2.get(ValueLayout.JAVA_BYTE, offset));
    }

    public static int compareMemorySegmentsUsingOffset(MemorySegment segment1,
                                                       MemorySegment segment2,
                                                       long segment2Offset,
                                                       long segment2Size) {
        long mismatch = MemorySegment.mismatch(segment1,
                0,
                segment1.byteSize(),
                segment2,
                segment2Offset,
                segment2Offset + segment2Size);
        if (mismatch == -1) {
            return 0;
        } else if (mismatch == segment1.byteSize()) {
            return -1;
        } else if (mismatch == segment2Size) {
            return 1;
        }
        return Byte.compare(segment1.get(ValueLayout.JAVA_BYTE, mismatch),
                segment2.get(ValueLayout.JAVA_BYTE, segment2Offset + mismatch));
    }
}
