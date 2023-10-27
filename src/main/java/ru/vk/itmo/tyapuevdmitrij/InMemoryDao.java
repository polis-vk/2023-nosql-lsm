package ru.vk.itmo.tyapuevdmitrij;

import ru.vk.itmo.BaseEntry;
import ru.vk.itmo.Config;
import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;

import java.io.File;
import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
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
    private Path ssTablePath;
    private final List<MemorySegment> ssTables;
    private static final String SS_TABLE_FILE_NAME = "ssTable";
    private final int ssTablesQuantity;

    private final Arena readArena;
    private final Arena writeArena;
    private final Arena compactionWriteArena;
    private final Config config;
    private long ssTablesEntryQuantity;
    private boolean compacted = false;

    public InMemoryDao() {
        ssTablePath = null;
        ssTables = null;
        ssTablesQuantity = 0;
        readArena = null;
        writeArena = null;
        config = null;
        compactionWriteArena = null;
    }

    public InMemoryDao(Config config) {
        this.config = config;
        ssTablesQuantity = findSsTablesQuantity(config);
        ssTables = new ArrayList<>(ssTablesQuantity);
        ssTablePath = config.basePath().resolve(SS_TABLE_FILE_NAME + ssTablesQuantity);
        writeArena = Arena.ofShared();
        readArena = Arena.ofShared();
        if (ssTablesQuantity != 0) {
            for (int i = 0; i < ssTablesQuantity; i++) {
                ssTablePath = config.basePath().resolve(SS_TABLE_FILE_NAME + i);
                ssTables.add(getReadBufferFromSsTable(ssTablePath));
            }
        }
        ssTablePath = config.basePath().resolve(SS_TABLE_FILE_NAME + ssTablesQuantity);
        compactionWriteArena = Arena.ofShared();
    }

    @Override
    public Iterator<Entry<MemorySegment>> get(MemorySegment from, MemorySegment to) {
        return range(getMemTableIterator(from, to), from, to);
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
        if (value != null || ssTables == null) {
            return value;
        }
        return getSsTableDataByKey(key);
    }

    @Override
    public void upsert(Entry<MemorySegment> entry) {
        memTable.put(entry.key(), entry);
    }

    @Override
    public void compact() throws IOException {
        if (ssTablesQuantity == 0 && memTable.isEmpty()) {
            return;
        }
        Iterator<Entry<MemorySegment>> dataIterator = get(null, null);
        MemorySegment buffer = getWriteBufferToCompaction();
        long bufferByteSize = buffer.byteSize();
        buffer.set(ValueLayout.JAVA_LONG_UNALIGNED, bufferByteSize - Long.BYTES, ssTablesEntryQuantity);
        long[] offsets = new long[2];
        offsets[1] = bufferByteSize - Long.BYTES - ssTablesEntryQuantity * 2L * Long.BYTES;
        while (dataIterator.hasNext()) {
            writeEntryAndIndexesToCompactionTable(buffer, dataIterator.next(), offsets);
        }
        compactionWriteArena.close();
        deleteOldSsTables();
        compacted = true;
    }

    private void deleteOldSsTables() {
        File directory = new File(config.basePath().toUri());
        if (directory.exists() && directory.isDirectory()) {
            if (directory.exists() && directory.isDirectory()) {
                File[] files = directory.listFiles();
                if (files != null) {
                    for (File file : files) {
                        if (!file.getName().contains(SS_TABLE_FILE_NAME + ssTablesQuantity)) {
                            boolean deleted = file.delete();
                            if (!deleted) {
                                throw new RuntimeException();
                            }
                        }
                    }
                }
            }
            File[] remainingFiles = directory.listFiles();
            if (remainingFiles != null && remainingFiles.length == 1) {
                File remainingFile = remainingFiles[0];
                String newFilePath = remainingFile.getParent() + File.separator + SS_TABLE_FILE_NAME + 0;
                boolean renamed = remainingFile.renameTo(new File(newFilePath));
                if (!renamed) {
                    throw new RuntimeException();
                }
            }

        }
    }

    private void writeEntryAndIndexesToCompactionTable(MemorySegment buffer,
                                                       Entry<MemorySegment> entry, long[] offsets) {
        buffer.set(ValueLayout.JAVA_LONG_UNALIGNED, offsets[1], offsets[0]);
        offsets[1] += Long.BYTES;
        buffer.set(ValueLayout.JAVA_LONG_UNALIGNED, offsets[0], entry.key().byteSize());
        offsets[0] += Long.BYTES;
        MemorySegment.copy(entry.key(), 0, buffer, offsets[0], entry.key().byteSize());
        offsets[0] += entry.key().byteSize();
        buffer.set(ValueLayout.JAVA_LONG_UNALIGNED, offsets[1], offsets[0]);
        offsets[1] += Long.BYTES;
        buffer.set(ValueLayout.JAVA_LONG_UNALIGNED, offsets[0], entry.value().byteSize());
        offsets[0] += Long.BYTES;
        MemorySegment.copy(entry.value(), 0, buffer, offsets[0], entry.value().byteSize());
        offsets[0] += entry.value().byteSize();
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

    @Override
    public void close() throws IOException {
        if (compacted) {
            return;
        }
        if (!readArena.scope().isAlive()) {
            return;
        }
        readArena.close();
        if (memTable.isEmpty()) {
            writeArena.close();
            return;
        }
        MemorySegment buffer = getWriteBufferToSsTable();
        writeMemTableDataToFile(buffer);
        writeArena.close();
    }

    private MemorySegment getWriteBufferToCompaction() throws IOException {
        ssTablePath = config.basePath().resolve(SS_TABLE_FILE_NAME + ssTablesQuantity);
        MemorySegment buffer;
        try (FileChannel channel = FileChannel.open(ssTablePath, EnumSet.of(StandardOpenOption.READ,
                StandardOpenOption.WRITE,
                StandardOpenOption.CREATE,
                StandardOpenOption.TRUNCATE_EXISTING))) {
            buffer = channel.map(FileChannel.MapMode.READ_WRITE, 0, getCompactionTableByteSize(), compactionWriteArena);
        }
        return buffer;
    }

    private long getSsTableDataByteSize() {
        long ssTableDataByteSize = 0;
        for (Map.Entry<MemorySegment, Entry<MemorySegment>> entry : memTable.entrySet()) {
            ssTableDataByteSize += entry.getKey().byteSize();
            if (entry.getValue().value() != null) {
                ssTableDataByteSize += entry.getValue().value().byteSize();
            }
        }
        return ssTableDataByteSize + memTable.size() * Long.BYTES * 4L + Long.BYTES;
    }

    private MemorySegment getWriteBufferToSsTable() throws IOException {
        MemorySegment buffer;
        try (FileChannel channel = FileChannel.open(ssTablePath, EnumSet.of(StandardOpenOption.READ,
                StandardOpenOption.WRITE,
                StandardOpenOption.CREATE,
                StandardOpenOption.TRUNCATE_EXISTING))) {
            buffer = channel.map(FileChannel.MapMode.READ_WRITE, 0, getSsTableDataByteSize(), writeArena);
        }
        return buffer;
    }

    private void writeMemTableDataToFile(MemorySegment buffer) {
        /*
         KeyByteSize|key|ValueByteSize|Value|...|0|KeyByteSizeOffset1|..
         .|memTableSize - 1|KeyByteSizeOffsetN|memTableSize
        */
        long offset = 0;
        long bufferByteSize = buffer.byteSize();
        long memTableSize = memTable.size();
        long writeIndexPosition = bufferByteSize - memTableSize * 2L * Long.BYTES - Long.BYTES;
        //write to the end of file size of memTable
        buffer.set(ValueLayout.JAVA_LONG_UNALIGNED, bufferByteSize - Long.BYTES, memTableSize);
        for (Entry<MemorySegment> entry : memTable.values()) {
            buffer.set(ValueLayout.JAVA_LONG_UNALIGNED, offset, entry.key().byteSize());
            //  write keyByteSizeOffsetPosition to the end of buffer
            buffer.set(ValueLayout.JAVA_LONG_UNALIGNED, writeIndexPosition, offset);
            writeIndexPosition += Long.BYTES;
            offset += Long.BYTES;
            MemorySegment.copy(entry.key(), 0, buffer, offset, entry.key().byteSize());
            offset += entry.key().byteSize();
            //  write valueByteSizeOffsetPosition to the end of buffer next to the keyByteSizeOffsetPosition
            buffer.set(ValueLayout.JAVA_LONG_UNALIGNED, writeIndexPosition, offset);
            writeIndexPosition += Long.BYTES;
            if (entry.value() == null) {
                buffer.set(ValueLayout.JAVA_LONG_UNALIGNED, offset, tombstone(offset));
                offset += Long.BYTES;
            } else {
                buffer.set(ValueLayout.JAVA_LONG_UNALIGNED, offset, entry.value().byteSize());
                offset += Long.BYTES;
                MemorySegment.copy(entry.value(), 0, buffer, offset, entry.value().byteSize());
                offset += entry.value().byteSize();
            }
        }
    }

    private MemorySegment getReadBufferFromSsTable(Path ssTablePath) {
        MemorySegment buffer;
        boolean created = false;
        try (FileChannel channel = FileChannel.open(ssTablePath, StandardOpenOption.READ)) {
            buffer = channel.map(FileChannel.MapMode.READ_ONLY, 0, Files.size(ssTablePath), readArena);
            created = true;
        } catch (IOException e) {
            buffer = null;
        } finally {
            if (!created) {
                readArena.close();
            }
        }

        return buffer;
    }

    private long getSsTableIndexByKey(MemorySegment ssTable, MemorySegment key) {
        long memTableSize = ssTable.get(ValueLayout.JAVA_LONG_UNALIGNED, ssTable.byteSize() - Long.BYTES);
        long left = 0;
        long right = memTableSize - 1L;
        long lastKeyIndexOffset = ssTable.byteSize() - 3 * Long.BYTES;
        while (left <= right) {
            long mid = (left + right) >>> 1;
            long midOffset = lastKeyIndexOffset - (memTableSize - 1L) * Long.BYTES * 2L + mid * 2L * Long.BYTES;
            MemorySegment readKey = getKeyByOffset(ssTable, midOffset);
            int res = memorySegmentComparator.compare(readKey, key);
            if (res == 0) {
                return mid;
            }
            if (res > 0) {
                right = mid - 1;
            } else {
                left = mid + 1;
            }
        }
        return left;
    }

    private Entry<MemorySegment> getSsTableEntryByIndex(MemorySegment ssTable, long index) {
        long memTableSize = ssTable.get(ValueLayout.JAVA_LONG_UNALIGNED, ssTable.byteSize() - Long.BYTES);
        if (index > memTableSize - 1 || index < 0) {
            return null;
        }
        long keyIndexOffset = ssTable.byteSize() - 3 * Long.BYTES
                - (memTableSize - 1L) * Long.BYTES * 2L + index * 2L * Long.BYTES;
        MemorySegment readKey = getKeyByOffset(ssTable, keyIndexOffset);
        MemorySegment readValue = getValueByOffset(ssTable, keyIndexOffset + Long.BYTES);
        return new BaseEntry<>(readKey, readValue);
    }

    private MemorySegment getKeyByOffset(MemorySegment ssTable, long offset) {
        long keyByteSizeOffset = ssTable.get(ValueLayout.JAVA_LONG_UNALIGNED, offset);
        long keyByteSize = ssTable.get(ValueLayout.JAVA_LONG_UNALIGNED, keyByteSizeOffset);
        long keyOffset = keyByteSizeOffset + Long.BYTES;
        return ssTable.asSlice(keyOffset, keyByteSize);
    }

    private MemorySegment getValueByOffset(MemorySegment ssTable, long offset) {
        long valueByteSizeOffset = ssTable.get(ValueLayout.JAVA_LONG_UNALIGNED, offset);
        long valueByteSize = ssTable.get(ValueLayout.JAVA_LONG_UNALIGNED, valueByteSizeOffset);
        if (valueByteSize < 0) {
            return null;
        }
        long valueOffset = valueByteSizeOffset + Long.BYTES;
        return ssTable.asSlice(valueOffset, valueByteSize);
    }

    private Entry<MemorySegment> getSsTableDataByKey(MemorySegment key) {
        /*
         KeyByteSize|key|ValueByteSize|Value|...|0|KeyByteSizeOffset1|..
         .|memTableSize - 1|KeyByteSizeOffsetN|memTableSize
        */
        for (int i = ssTables.size() - 1; i > -1; i--) {
            MemorySegment ssTable = ssTables.get(i);
            long memTableSize = ssTable.get(ValueLayout.JAVA_LONG_UNALIGNED, ssTable.byteSize() - Long.BYTES);
            long left = 0;
            long right = memTableSize - 1L;
            long lastKeyIndexOffset = ssTable.byteSize() - 3 * Long.BYTES;
            while (left <= right) {
                long mid = (right + left) >>> 1;
                long midOffset = lastKeyIndexOffset - (memTableSize - 1L) * Long.BYTES * 2L + mid * 2L * Long.BYTES;
                MemorySegment readKey = getKeyByOffset(ssTable, midOffset);
                int res = memorySegmentComparator.compare(readKey, key);
                if (res == 0) {
                    MemorySegment value = getValueByOffset(ssTable, midOffset + Long.BYTES);
                    if (value == null) {
                        return null;
                    }
                    return new BaseEntry<>(key, value);
                } else if (res > 0) {
                    right = mid - 1;
                } else {
                    left = mid + 1;
                }
            }
        }
        return null;
    }

    private int findSsTablesQuantity(Config config) {
        File dir = new File(config.basePath().toUri());
        File[] files = dir.listFiles();
        if (files != null) {
            long ssTablesQuantity = Arrays.stream(files)
                    .filter(file -> file.isFile() && file.getName().contains(SS_TABLE_FILE_NAME))
                    .count();
            return (int) ssTablesQuantity;
        } else return 0;
    }

    private Iterator<Entry<MemorySegment>> range(
            Iterator<Entry<MemorySegment>> firstIterator,
            MemorySegment from,
            MemorySegment to) {
        List<Iterator<Entry<MemorySegment>>> iterators = new ArrayList<>(ssTablesQuantity + 1);
        for (MemorySegment memorySegment : ssTables) {
            iterators.add(iterator(memorySegment, from, to));
        }
        iterators.add(firstIterator);

        return new MergeIterator<>(iterators, Comparator.comparing(Entry::key, memorySegmentComparator)) {
            @Override
            protected boolean skip(Entry<MemorySegment> memorySegmentEntry) {
                return memorySegmentEntry.value() == null;
            }
        };
    }

    private Iterator<Entry<MemorySegment>> iterator(MemorySegment ssTable, MemorySegment from, MemorySegment to) {
        long recordIndexFrom = from == null ? 0 : normalize(getSsTableIndexByKey(ssTable, from));
        long memTableSize = ssTable.get(ValueLayout.JAVA_LONG_UNALIGNED, ssTable.byteSize() - Long.BYTES);
        long recordIndexTo = to == null ? memTableSize : normalize(getSsTableIndexByKey(ssTable, to));

        return new Iterator<>() {
            long index = recordIndexFrom;

            @Override
            public boolean hasNext() {
                return index < recordIndexTo;
            }

            @Override
            public Entry<MemorySegment> next() {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }
                Entry<MemorySegment> entry = getSsTableEntryByIndex(ssTable, index);
                index++;
                return entry;
            }
        };
    }

    private static long tombstone(long offset) {
        return 1L << 63 | offset;
    }

    private static long normalize(long value) {
        return value & ~(1L << 63);
    }
}
