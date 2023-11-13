package ru.vk.itmo.tyapuevdmitrij;

import ru.vk.itmo.Config;
import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class MemorySegmentDao implements Dao<MemorySegment, Entry<MemorySegment>> {
    private static final Comparator<MemorySegment> MEMORY_SEGMENT_COMPARATOR = (segment1, segment2) -> {
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
            new ConcurrentSkipListMap<>(MEMORY_SEGMENT_COMPARATOR);

    private final Arena readArena;
    private final Path ssTablePath;
    private long ssTablesEntryQuantity;
    private boolean compacted;
    private final Storage storage;

    public MemorySegmentDao() {
        ssTablePath = null;
        readArena = null;
        storage = null;
    }

    public MemorySegmentDao(Config config) {
        ssTablePath = config.basePath();
        readArena = Arena.ofShared();
        storage = new Storage(ssTablePath, readArena);
    }

    @Override
    public Iterator<Entry<MemorySegment>> get(MemorySegment from, MemorySegment to) {
        return storage.range(getMemTableIterator(from, to), from, to, MEMORY_SEGMENT_COMPARATOR);
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
        if (value != null && value.value() == null) {
            return null;
        }
        if (value != null || storage.ssTables == null) {
            return value;
        }
        Iterator<Entry<MemorySegment>> iterator = storage.range(Collections.emptyIterator(),
                key,
                null,
                MEMORY_SEGMENT_COMPARATOR);

        if (!iterator.hasNext()) {
            return null;
        }
        Entry<MemorySegment> next = iterator.next();
        if (MEMORY_SEGMENT_COMPARATOR.compare(next.key(), key) == 0) {
            return next;
        }
        return null;
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
        Arena writeArena = Arena.ofConfined();
        MemorySegment buffer = NmapBuffer.getWriteBufferToSsTable(getCompactionTableByteSize(),
                ssTablePath,
                storage.ssTablesQuantity,
                writeArena,
                true);
        long bufferByteSize = buffer.byteSize();
        buffer.set(ValueLayout.JAVA_LONG_UNALIGNED, bufferByteSize - Long.BYTES, ssTablesEntryQuantity);
        long dataOffset = 0;
        long indexOffset = bufferByteSize - Long.BYTES - ssTablesEntryQuantity * 2L * Long.BYTES;
        while (dataIterator.hasNext()) {
            Entry<MemorySegment> entry = dataIterator.next();
            buffer.set(ValueLayout.JAVA_LONG_UNALIGNED, indexOffset, dataOffset);
            indexOffset += Long.BYTES;
            buffer.set(ValueLayout.JAVA_LONG_UNALIGNED, dataOffset, entry.key().byteSize());
            dataOffset += Long.BYTES;
            MemorySegment.copy(entry.key(), 0, buffer, dataOffset, entry.key().byteSize());
            dataOffset += entry.key().byteSize();
            buffer.set(ValueLayout.JAVA_LONG_UNALIGNED, indexOffset, dataOffset);
            indexOffset += Long.BYTES;
            buffer.set(ValueLayout.JAVA_LONG_UNALIGNED, dataOffset, entry.value().byteSize());
            dataOffset += Long.BYTES;
            MemorySegment.copy(entry.value(), 0, buffer, dataOffset, entry.value().byteSize());
            dataOffset += entry.value().byteSize();
        }
        if (writeArena.scope().isAlive()) {
            writeArena.close();
        }
        StorageHelper.deleteOldSsTables(ssTablePath);
        StorageHelper.renameCompactedSsTable(ssTablePath);
        compacted = true;
    }

    @Override
    public void close() throws IOException {
        if (compacted) {
            readArena.close();
            return;
        }
        if (memTable.isEmpty()) {
            readArena.close();
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
