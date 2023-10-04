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
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListMap;

import static java.nio.channels.FileChannel.MapMode.READ_ONLY;
import static java.nio.channels.FileChannel.MapMode.READ_WRITE;
import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.WRITE;

public class InMemoryDao implements Dao<MemorySegment, Entry<MemorySegment>> {

    private final Comparator<MemorySegment> comparator = InMemoryDao::compare;

    private final ConcurrentSkipListMap<MemorySegment, Entry<MemorySegment>> storage
            = new ConcurrentSkipListMap<>(comparator);

    private final ConcurrentSkipListMap<MemorySegment, Entry<MemorySegment>> cachedValues
            = new ConcurrentSkipListMap<>(comparator);

    private final Arena arena;
    private final Path path;
    private static final String FILE_NAME = "sstable.txt";

    public InMemoryDao() {
        path = Path.of("C:\\Users\\dimit\\AppData\\Local\\Temp");
        arena = Arena.ofConfined();
    }

    public InMemoryDao(Config config) {
        path = config.basePath().resolve(FILE_NAME);
        arena = Arena.ofConfined();
    }

    @Override
    public Entry<MemorySegment> get(MemorySegment key) {
        Entry<MemorySegment> entry = storage.get(key);
        // avoiding extra file operations by checking cached values
        if (entry == null) {
            entry = cachedValues.get(key);
        }
        // if value is still null then searching in file
        if (entry == null && path.toFile().exists()) {
            try (FileChannel fc = FileChannel.open(path, Set.of(CREATE, READ))) {
                if (fc.size() != 0) {
                    MemorySegment mappedSegment = fc.map(READ_ONLY, 0, fc.size(), arena);
                    entry = searchInSlice(mappedSegment, key);
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
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
    public void flush() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void close() {
        try (FileChannel fc = FileChannel.open(path, Set.of(CREATE, READ, WRITE))) {

            long ssTableSize = Long.BYTES * 2L * storage.size();
            for (Entry<MemorySegment> value : storage.values()) {
                ssTableSize += value.key().byteSize() + value.value().byteSize();
            }

            MemorySegment ssTable = fc.map(READ_WRITE, 0, ssTableSize, arena);
            long offset = 0;

            for (Entry<MemorySegment> value : storage.values()) {
                ssTable.set(ValueLayout.JAVA_LONG_UNALIGNED, offset, value.key().byteSize());
                offset += Long.BYTES;
                MemorySegment.copy(value.key(), 0, ssTable, offset, value.key().byteSize());
                offset += value.key().byteSize();

                ssTable.set(ValueLayout.JAVA_LONG_UNALIGNED, offset, value.value().byteSize());
                offset += Long.BYTES;
                MemorySegment.copy(value.value(), 0, ssTable, offset, value.value().byteSize());
                offset += value.value().byteSize();
            }
            arena.close();

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private Entry<MemorySegment> searchInSlice(MemorySegment mappedSegment, MemorySegment key) {
        long offset = 0;
        while (offset < mappedSegment.byteSize() - Long.BYTES) {

            long size = mappedSegment.get(ValueLayout.JAVA_LONG_UNALIGNED, offset);
            offset += Long.BYTES;
            MemorySegment slicedKey = mappedSegment.asSlice(offset, size);
            offset += size;

            size = mappedSegment.get(ValueLayout.JAVA_LONG_UNALIGNED, offset);
            offset += Long.BYTES;

            if (compare(key, slicedKey) == 0) {
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
