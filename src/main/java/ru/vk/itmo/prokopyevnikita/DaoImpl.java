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
import java.nio.file.NoSuchFileException;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Iterator;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class DaoImpl implements Dao<MemorySegment, Entry<MemorySegment>> {

    private final Config config;
    private final MemorySegment mappedFile;
    private final Arena arena;
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    private final ConcurrentSkipListMap<MemorySegment, Entry<MemorySegment>> map =
            new ConcurrentSkipListMap<>((o1, o2) -> {
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
            });

    public DaoImpl(Config config) throws IOException {
        this.config = config;
        Path path = getFilePath();
        MemorySegment memorySegment;
        arena = Arena.ofShared();
        try (FileChannel fileChannel = getFileChannel(StandardOpenOption.READ)) {
            long size = Files.size(path);
            memorySegment = fileChannel.map(FileChannel.MapMode.READ_ONLY, 0, size, arena);
        } catch (NoSuchFileException e) {
            memorySegment = null;
        }
        mappedFile = memorySegment;
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

            if (mappedFile == null) {
                return null;
            }

            long offset = 0;
            while (offset < mappedFile.byteSize()) {
                long keySize = mappedFile.get(ValueLayout.JAVA_LONG_UNALIGNED, offset);
                offset += Long.BYTES;
                long valueSize = mappedFile.get(ValueLayout.JAVA_LONG_UNALIGNED, offset);
                offset += Long.BYTES;

                if (keySize != key.byteSize()) {
                    offset += keySize + valueSize;
                    continue;
                }

                MemorySegment keySegment = mappedFile.asSlice(offset, keySize);
                offset += keySize;

                if (key.mismatch(keySegment) == -1) {
                    return new BaseEntry<>(keySegment, mappedFile.asSlice(offset, valueSize));
                }

                offset += valueSize;
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
        if (!arena.scope().isAlive()) {
            return;
        }
        arena.close();

        lock.writeLock().lock();
        try (Arena writeArena = Arena.ofConfined()) {
            long size = 0;
            for (Entry<MemorySegment> entry : map.values()) {
                size += entry.key().byteSize() + entry.value().byteSize();
            }

            size += Long.BYTES * map.size() * 2L;

            MemorySegment memorySegment = getFileChannel(
                    StandardOpenOption.READ,
                    StandardOpenOption.WRITE,
                    StandardOpenOption.CREATE,
                    StandardOpenOption.TRUNCATE_EXISTING)
                    .map(FileChannel.MapMode.READ_WRITE, 0, size, writeArena);

            long offset = 0;
            for (Entry<MemorySegment> entry : map.values()) {
                memorySegment.set(ValueLayout.JAVA_LONG_UNALIGNED, offset, entry.key().byteSize());
                offset += Long.BYTES;
                memorySegment.set(ValueLayout.JAVA_LONG_UNALIGNED, offset, entry.value().byteSize());
                offset += Long.BYTES;
                memorySegment.asSlice(offset).copyFrom(entry.key());
                offset += entry.key().byteSize();
                memorySegment.asSlice(offset).copyFrom(entry.value());
                offset += entry.value().byteSize();
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    private FileChannel getFileChannel(OpenOption... openOption) throws IOException {
        return FileChannel.open(getFilePath(), openOption);
    }

    private Path getFilePath() {
        return config.basePath().resolve("data.db");
    }
}
