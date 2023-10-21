package ru.vk.itmo.osokindmitry;

import ru.vk.itmo.BaseEntry;
import ru.vk.itmo.Config;
import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Collections;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class InMemoryDao implements Dao<MemorySegment, Entry<MemorySegment>> {

    private final ConcurrentNavigableMap<MemorySegment, Entry<MemorySegment>> storage;
    private final ConcurrentNavigableMap<MemorySegment, Entry<MemorySegment>> cachedValues;

    private final Arena arena;
    private final Path path;
    private final MemorySegment mappedFile;

    private static final String FILE_NAME = "sstable.txt";

    public InMemoryDao(Config config) {
        path = config.basePath().resolve(FILE_NAME);
        arena = Arena.ofConfined();
        storage = new ConcurrentSkipListMap<>(InMemoryDao::compare);
        cachedValues = new ConcurrentSkipListMap<>(InMemoryDao::compare);
        mappedFile = mapFile(path);
    }

    @Override
    public Entry<MemorySegment> get(MemorySegment key) {
        Entry<MemorySegment> entry = storage.get(key);
        // avoiding extra file operations by checking cached values
        if (entry == null) {
            entry = cachedValues.get(key);
        }
        // if value is still null then searching in file
        if (entry == null && mappedFile != null) {
            entry = searchInSlice(mappedFile, key);
        }

        return entry;
    }

    @Override
    public Iterator<Entry<MemorySegment>> get(MemorySegment from, MemorySegment to) {
        if (storage.isEmpty()) {
            return Collections.emptyIterator();
        }
        boolean empty = to == null;
        MemorySegment first = from == null ? storage.firstKey() : from;
        MemorySegment last = to == null ? storage.lastKey() : to;
        return storage.subMap(first, true, last, empty).values().iterator();
    }

    @Override
    public void upsert(Entry<MemorySegment> entry) {
        storage.put(entry.key(), entry);
    }

    @Override
    public void flush() throws IOException {
        try (
                FileChannel fc = FileChannel.open(
                        path,
                        Set.of(StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE))
        ) {

            long ssTableSize = Long.BYTES * 2L * storage.size();
            for (Entry<MemorySegment> value : storage.values()) {
                ssTableSize += value.key().byteSize() + value.value().byteSize();
            }

            MemorySegment ssTable = fc.map(FileChannel.MapMode.READ_WRITE, 0, ssTableSize, arena);
            long offset = 0;

            for (Entry<MemorySegment> value : storage.values()) {
                offset = writeEntry(value.key(), ssTable, offset);
                offset = writeEntry(value.value(), ssTable, offset);
            }
        }
    }

    @Override
    public void close() throws IOException {
        flush();
        if (arena.scope().isAlive()) {
            arena.close();
        }
    }

    private long writeEntry(MemorySegment entry, MemorySegment ssTable, long offset) {
        long curOffset = offset;
        ssTable.set(ValueLayout.JAVA_LONG_UNALIGNED, curOffset, entry.byteSize());
        curOffset += Long.BYTES;
        MemorySegment.copy(entry, 0, ssTable, curOffset, entry.byteSize());
        curOffset += entry.byteSize();
        return curOffset;
    }

    private MemorySegment mapFile(Path path) {
        if (path.toFile().exists()) {
            try (
                    FileChannel fc = FileChannel.open(
                            path,
                            Set.of(StandardOpenOption.CREATE, StandardOpenOption.READ))
            ) {
                if (fc.size() != 0) {
                    return fc.map(FileChannel.MapMode.READ_ONLY, 0, fc.size(), arena);
                }
            } catch (IOException e) {
                return null;
            }
        }
        return null;
    }

    private Entry<MemorySegment> searchInSlice(MemorySegment mappedSegment, MemorySegment key) {
        long offset = 0;
        while (offset < mappedSegment.byteSize() - Long.BYTES) {

            long size = mappedSegment.get(ValueLayout.JAVA_LONG_UNALIGNED, offset);
            offset += Long.BYTES;
            MemorySegment slicedKey = mappedSegment.asSlice(offset, size);
            offset += size;

            long mismatch = key.mismatch(slicedKey);
            size = mappedSegment.get(ValueLayout.JAVA_LONG_UNALIGNED, offset);
            offset += Long.BYTES;

            if (mismatch == -1) {
                MemorySegment slicedValue = mappedSegment.asSlice(offset, size);
                BaseEntry<MemorySegment> entry = new BaseEntry<>(slicedKey, slicedValue);
                cachedValues.put(slicedKey, entry);
                return entry;
            }
            offset += size;
        }
        return null;
    }

    private static int compare(MemorySegment segment1, MemorySegment segment2) {
        long offset = segment1.mismatch(segment2);
        if (offset == -1) {
            return 0;
        } else if (offset == segment1.byteSize()) {
            return -1;
        } else if (offset == segment2.byteSize()) {
            return 1;
        }
        byte b1 = segment1.get(ValueLayout.JAVA_BYTE, offset);
        byte b2 = segment2.get(ValueLayout.JAVA_BYTE, offset);
        return Byte.compare(b1, b2);
    }

}
