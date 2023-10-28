package ru.vk.itmo.tyapuevdmitrij;

import ru.vk.itmo.Config;
import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.Iterator;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class InMemoryDao implements Dao<MemorySegment, Entry<MemorySegment>> {

    private static final Comparator<MemorySegment> memorySegmentComparator = (segment1, segment2) -> {
        long offset = segment1.mismatch(segment2);
        if (offset == -1) {
            return 0;
        }
        if (offset == segment1.byteSize()) {
            return -1;
        }
        if (offset == segment2.byteSize()) {
            return 1;
        }
        return segment1.get(ValueLayout.JAVA_BYTE, offset) - segment2.get(ValueLayout.JAVA_BYTE, offset);
    };
    private final ConcurrentNavigableMap<MemorySegment, Entry<MemorySegment>> memTable =
            new ConcurrentSkipListMap<>(memorySegmentComparator);

    private final Arena readArena;
    private final Path ssTablePath;
    private long ssTablesEntryQuantity;
    private boolean compacted;
    private final Storage storage;

    public InMemoryDao() {
        ssTablePath = null;
        readArena = null;
        storage = null;
    }

    public InMemoryDao(Config config) {
        ssTablePath = config.basePath();
        readArena = Arena.ofShared();
        storage = new Storage(ssTablePath, readArena);
    }

    @Override
    public Iterator<Entry<MemorySegment>> get(MemorySegment from, MemorySegment to) {
        return storage.range(getMemTableIterator(from, to), from, to, memorySegmentComparator);
    }

    private Iterator<Entry<MemorySegment>> getMemTableIterator(MemorySegment from, MemorySegment to) {
        if (from == null && to == null) {
            return memTable.values().iterator();
        }
        if (from == null) {
            return memTable.headMap(to).values().iterator();
        }
        if (to == null) {
            return memTable.tailMap(from).values().iterator();
        }
        return memTable.subMap(from, to).values().iterator();
    }

    @Override
    public Entry<MemorySegment> get(MemorySegment key) {
        Entry<MemorySegment> value = memTable.get(key);
        if (memTable.containsKey(key) && value.value() == null) {
            return null;
        }
        if (value != null || storage.ssTables == null) {
            return value;
        }
        return storage.getSsTableDataByKey(key, memorySegmentComparator);
    }

    @Override
    public void upsert(Entry<MemorySegment> entry) {
        memTable.put(entry.key(), entry);
    }

    @Override
    public void compact() throws IOException {
        if (storage.ssTablesQuantity == 0 && memTable.isEmpty()) {
            return;
        }
        Iterator<Entry<MemorySegment>> dataIterator = get(null, null);
        MemorySegment buffer = NmapBuffer.getWriteBufferToSsTable(getCompactionTableByteSize(),
                ssTablePath,
                storage.ssTablesQuantity);
        long bufferByteSize = buffer.byteSize();
        buffer.set(ValueLayout.JAVA_LONG_UNALIGNED, bufferByteSize - Long.BYTES, ssTablesEntryQuantity);
        long[] offsets = new long[2];
        offsets[1] = bufferByteSize - Long.BYTES - ssTablesEntryQuantity * 2L * Long.BYTES;
        while (dataIterator.hasNext()) {
            storage.writeEntryAndIndexesToCompactionTable(buffer, dataIterator.next(), offsets);
        }
        StorageHelper.deleteOldSsTables(ssTablePath, storage.ssTablesQuantity);
        StorageHelper.renameCompactedSsTable(ssTablePath);
        compacted = true;
    }

    @Override
    public void close() throws IOException {
        if (compacted) {
            return;
        }
        if (memTable.isEmpty()) {
            return;
        }
        if (!readArena.scope().isAlive()) {
            return;
        }
        readArena.close();
        storage.save(memTable.values(), ssTablePath);

    }

    private long getCompactionTableByteSize() {
        Iterator<Entry<MemorySegment>> dataIterator = get(null, null);
        long compactionTableByteSize = 0;
        long countEntry = 0;
        while (dataIterator.hasNext()) {
            Entry<MemorySegment> entry = dataIterator.next();
            compactionTableByteSize += entry.key().byteSize();
            compactionTableByteSize += entry.value().byteSize();
            countEntry++;
        }
        ssTablesEntryQuantity = countEntry;
        return compactionTableByteSize + countEntry * 4L * Long.BYTES + Long.BYTES;
    }

}
