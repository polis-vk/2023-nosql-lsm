package ru.vk.itmo.tyapuevdmitrij;

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
import java.util.Comparator;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class InMemoryDao implements Dao<MemorySegment, Entry<MemorySegment>> {
    private final Comparator<MemorySegment> memorySegmentComparator = (segment1, segment2) -> {
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
    private final ConcurrentNavigableMap<MemorySegment, Entry<MemorySegment>> memTable
            = new ConcurrentSkipListMap<>(memorySegmentComparator);
    private final Path ssTablePath;
    private final MemorySegment ssTable;
    private static final String SS_TABLE_FILE_NAME = "ssTable";

    public InMemoryDao() {
        ssTablePath = null;
        ssTable = null;
    }

    public InMemoryDao(Config config) {
        ssTablePath = config.basePath().resolve(SS_TABLE_FILE_NAME);
        ssTable = getReadBufferFromSsTable();
    }

    @Override
    public Iterator<Entry<MemorySegment>> get(MemorySegment from, MemorySegment to) {
        if (from == null && to == null) {
            return memTable.values().iterator();
        } else if (from == null) {
            return memTable.headMap(to).values().iterator();
        } else if (to == null) {
            return memTable.tailMap(from).values().iterator();
        }
        return memTable.subMap(from, to).values().iterator();
    }

    @Override
    public Entry<MemorySegment> get(MemorySegment key) {
        Entry<MemorySegment> value = memTable.get(key);
        if (value != null || ssTable == null) {
            return value;
        }
        return getSsTableDataByKey(key);
    }

    @Override
    public void upsert(Entry<MemorySegment> entry) {
        memTable.put(entry.key(), entry);
    }

    @Override
    public void flush() throws IOException {
        throw new UnsupportedOperationException("");
    }

    @Override
    public void close() throws IOException {
        MemorySegment buffer = getWriteBufferToSsTable();
        writeMemTableDataToFile(buffer);
    }

    public long getSsTableDataByteSize() {
        long ssTableDataByteSize = 0;
        for (Map.Entry<MemorySegment, Entry<MemorySegment>> entry : memTable.entrySet()) {
            ssTableDataByteSize += entry.getKey().byteSize();
            ssTableDataByteSize += entry.getValue().value().byteSize();
        }
        return ssTableDataByteSize + memTable.size() * Long.BYTES * 2L;
    }

    public MemorySegment getWriteBufferToSsTable() throws IOException {
        MemorySegment buffer;
        try (FileChannel channel = FileChannel.open(ssTablePath, EnumSet.of(
                StandardOpenOption.READ,
                StandardOpenOption.WRITE,
                StandardOpenOption.CREATE,
                StandardOpenOption.TRUNCATE_EXISTING))) {
            buffer = channel.map(FileChannel.MapMode.READ_WRITE, 0, getSsTableDataByteSize(), Arena.ofAuto());
        }
        return buffer;
    }

    public void writeMemTableDataToFile(MemorySegment buffer) {
        long offset = 0;
        for (Entry<MemorySegment> entry : memTable.values()) {
            buffer.set(ValueLayout.JAVA_LONG_UNALIGNED, offset, entry.key().byteSize());
            offset += Long.BYTES;
            MemorySegment.copy(entry.key(), 0, buffer, offset, entry.key().byteSize());
            offset += entry.key().byteSize();
            buffer.set(ValueLayout.JAVA_LONG_UNALIGNED, offset, entry.value().byteSize());
            offset += Long.BYTES;
            MemorySegment.copy(entry.value(), 0, buffer, offset, entry.value().byteSize());
            offset += entry.value().byteSize();
        }
    }

    public final MemorySegment getReadBufferFromSsTable() {
        MemorySegment buffer;
        try (FileChannel channel = FileChannel.open(ssTablePath, StandardOpenOption.READ)) {
            buffer = channel.map(FileChannel.MapMode.READ_ONLY, 0, Files.size(ssTablePath), Arena.ofAuto());
        } catch (IOException e) {
            buffer = null;
        }
        return buffer;
    }

    public Entry<MemorySegment> getSsTableDataByKey(MemorySegment key) {
        long offset = 0;
        MemorySegment valueFromSsTable;
        while (offset < ssTable.byteSize()) {
            long keyByteSize = ssTable.get(ValueLayout.JAVA_LONG_UNALIGNED, offset);
            offset += Long.BYTES;
            long keysMismatch;
            keysMismatch = MemorySegment.mismatch(ssTable, offset, offset + keyByteSize, key, 0, key.byteSize());
            offset += keyByteSize;
            long valueByteSize = ssTable.get(ValueLayout.JAVA_LONG_UNALIGNED, offset);
            offset += Long.BYTES;
            valueFromSsTable = ssTable.asSlice(offset, valueByteSize);
            if (keysMismatch == -1) {
                return new BaseEntry<>(key, valueFromSsTable);
            }
            offset += valueByteSize;
        }
        return null; 
    }
}
