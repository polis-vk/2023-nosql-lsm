package ru.vk.itmo.abramovilya;

import ru.vk.itmo.Config;
import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.Iterator;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class DaoImpl implements Dao<MemorySegment, Entry<MemorySegment>> {
    private final ConcurrentNavigableMap<MemorySegment, Entry<MemorySegment>> map =
            new ConcurrentSkipListMap<>(DaoImpl::compareMemorySegments);
    private final Arena arena = Arena.ofShared();
    private final Storage storage;

    public DaoImpl(Config config) throws IOException {
        storage = new Storage(config, arena);
    }

    @Override
    public Iterator<Entry<MemorySegment>> get(MemorySegment from, MemorySegment to) {
        return new DaoIterator(
                storage.getTotalSStables(),
                from,
                to,
                storage,
                map
        );
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
        return storage.get(key);
    }

    @Override
    public void upsert(Entry<MemorySegment> entry) {
        map.put(entry.key(), entry);
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

    @Override
    public void flush() throws IOException {
        if (!map.isEmpty()) {
            writeMapIntoFile();
            storage.incTotalSStablesAmount();
        }
    }

    private void writeMapIntoFile() throws IOException {
        if (map.isEmpty()) {
            return;
        }
        int totalSStables = storage.getTotalSStables();
        storage.writeIteratorIntoFile(
                mapByteSizeInFile(),
                indexByteSizeInFile(),
                map.values().iterator(),
                storage.storagePath.resolve(Storage.SSTABLE_BASE_NAME + totalSStables),
                storage.storagePath.resolve(Storage.INDEX_BASE_NAME + totalSStables)
        );
    }

    private long mapByteSizeInFile() {
        long size = 0;
        for (var entry : map.values()) {
            size += 2 * Long.BYTES;
            size += entry.key().byteSize();
            if (entry.value() != null) {
                size += entry.value().byteSize();
            }
        }
        return size;
    }

    private long indexByteSizeInFile() {
        return (long) map.size() * (Integer.BYTES + Long.BYTES);
    }

    @Override
    public void close() throws IOException {
        if (arena.scope().isAlive()) {
            arena.close();
        }
        flush();
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
}
