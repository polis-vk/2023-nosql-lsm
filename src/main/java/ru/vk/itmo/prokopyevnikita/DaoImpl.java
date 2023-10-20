package ru.vk.itmo.prokopyevnikita;

import ru.vk.itmo.BaseEntry;
import ru.vk.itmo.Config;
import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Iterator;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class DaoImpl implements Dao<MemorySegment, Entry<MemorySegment>> {

    private final Config config;
    private final MemorySegment mappedDB;
    private final MemorySegment mappedIndex;
    private final Arena arena;
    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    private static final String DB = "data";
    private static final String INDEX = DB + "_index";

    private final NavigableMap<MemorySegment, Entry<MemorySegment>> map =
            new ConcurrentSkipListMap<>(DaoImpl::compare);

    public DaoImpl(Config config) throws IOException {
        this.config = config;

        Path pathDB = getFilePath(DB);
        Path pathIndex = getFilePath(INDEX);

        if (!Files.exists(pathDB)) {
            mappedDB = null;
            mappedIndex = null;
            arena = null;
            return;
        }

        arena = Arena.ofShared();
        try (
                FileChannel dbChannel = getFileChannelRead(pathDB);
                FileChannel indexChannel = getFileChannelRead(pathIndex)
        ) {
            mappedDB = dbChannel.map(FileChannel.MapMode.READ_ONLY, 0, Files.size(pathDB), arena);
            mappedIndex = indexChannel.map(FileChannel.MapMode.READ_ONLY, 0, Files.size(pathIndex), arena);
        }
    }

    @Override
    public Iterator<Entry<MemorySegment>> get(MemorySegment from, MemorySegment to) {
        if (from == null && to == null) {
            return map.values().iterator();
        } else if (to == null) {
            return map.tailMap(from).values().iterator();
        } else if (from == null) {
            return map.headMap(to).values().iterator();
        }
        return map.subMap(from, to).values().iterator();
    }

    @Override
    public Entry<MemorySegment> get(MemorySegment key) {
        lock.readLock().lock();
        try {
            Entry<MemorySegment> entry = map.get(key);
            if (entry != null) {
                return entry;
            }

            if (mappedDB == null) {
                return null;
            }

            long start = 0;
            long end = mappedIndex.byteSize() / Long.BYTES - 1;
            while (start <= end) {
                long mid = start + (end - start) / 2;

                long offset = mappedIndex.get(ValueLayout.JAVA_LONG_UNALIGNED, mid * Long.BYTES);

                long keySize = mappedDB.get(ValueLayout.JAVA_LONG_UNALIGNED, offset);
                offset += Long.BYTES;

                MemorySegment memorySegmentKey = mappedDB.asSlice(offset, keySize);
                offset += keySize;

                int cmp = compare(memorySegmentKey, key);
                if (cmp == 0) {
                    long valueSize = mappedDB.get(ValueLayout.JAVA_LONG_UNALIGNED, offset);
                    offset += Long.BYTES;

                    MemorySegment memorySegmentValue = mappedDB.asSlice(offset, valueSize);

                    return new BaseEntry<>(memorySegmentKey, memorySegmentValue);
                } else if (cmp < 0) {
                    start = mid + 1;
                } else {
                    end = mid - 1;
                }
            }

            return null;
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public void upsert(Entry<MemorySegment> entry) {
        if (entry == null) {
            throw new IllegalArgumentException("Value cannot be null");
        }
        lock.readLock().lock();
        try {
            map.put(entry.key(), entry);
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public void flush() {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public void close() throws IOException {
        if (arena != null) {
            if (!arena.scope().isAlive()) {
                return;
            }
            arena.close();
        }

        lock.writeLock().lock();
        try (
                Arena writeArena = Arena.ofConfined();
                FileChannel dbChannel = getFileChannelWrite(getFilePath(DB));
                FileChannel indexChannel = getFileChannelWrite(getFilePath(INDEX))
        ) {
            long dbSize = 0;
            long indexSize = (long) map.size() * Long.BYTES;
            for (Entry<MemorySegment> entry : map.values()) {
                dbSize += entry.key().byteSize() + entry.value().byteSize();
            }

            dbSize += Long.BYTES * map.size() * 2L;

            MemorySegment memorySegmentDB = dbChannel
                    .map(FileChannel.MapMode.READ_WRITE, 0, dbSize, writeArena);
            MemorySegment memorySegmentIndex = indexChannel
                    .map(FileChannel.MapMode.READ_WRITE, 0, indexSize, writeArena);

            long dbOffset = 0;
            long indexOffset = 0;
            for (Entry<MemorySegment> entry : map.values()) {
                memorySegmentIndex.set(ValueLayout.JAVA_LONG_UNALIGNED, indexOffset, dbOffset);
                indexOffset += Long.BYTES;

                memorySegmentDB.set(ValueLayout.JAVA_LONG_UNALIGNED, dbOffset, entry.key().byteSize());
                dbOffset += Long.BYTES;
                memorySegmentDB.asSlice(dbOffset).copyFrom(entry.key());
                dbOffset += entry.key().byteSize();
                memorySegmentDB.set(ValueLayout.JAVA_LONG_UNALIGNED, dbOffset, entry.value().byteSize());
                dbOffset += Long.BYTES;
                memorySegmentDB.asSlice(dbOffset).copyFrom(entry.value());
                dbOffset += entry.value().byteSize();
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    private FileChannel getFileChannelRead(Path path) throws IOException {
        return FileChannel.open(path, StandardOpenOption.READ);
    }

    private FileChannel getFileChannelWrite(Path path) throws IOException {
        return FileChannel.open(path,
                StandardOpenOption.READ,
                StandardOpenOption.WRITE,
                StandardOpenOption.CREATE,
                StandardOpenOption.TRUNCATE_EXISTING);
    }

    private Path getFilePath(String file) {
        return config.basePath().resolve(file);
    }

    public static int compare(MemorySegment o1, MemorySegment o2) {
        long relativeOffset = o1.mismatch(o2);
        if (relativeOffset == -1) {
            return 0;
        } else if (relativeOffset == o1.byteSize()) {
            return -1;
        } else if (relativeOffset == o2.byteSize()) {
            return 1;
        }
        return Byte.compare(
                o1.get(ValueLayout.JAVA_BYTE, relativeOffset),
                o2.get(ValueLayout.JAVA_BYTE, relativeOffset)
        );
    }
}
