package ru.vk.itmo.kislovdanil;

import ru.vk.itmo.BaseEntry;
import ru.vk.itmo.Entry;
import ru.vk.itmo.kislovdanil.iterators.DatabaseIterator;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.NavigableMap;

public class SSTable implements Comparable<SSTable> {
    // Contains offset and size for every key in index file
    private MemorySegment summaryFile;
    // Contains keys and for each key contains offset and size of assigned value
    private MemorySegment indexFile;
    // Contains values
    private MemorySegment dataFile;
    private final Comparator<MemorySegment> memSegComp;
    private final Arena filesArena;
    private final long tableId;

    private final long size;

    public SSTable(Path basePath, Comparator<MemorySegment> memSegComp, long tableId,
                   NavigableMap<MemorySegment, Entry<MemorySegment>> memTable, boolean rewrite, Arena filesArena) throws IOException {
        this.tableId = tableId;
        this.filesArena = filesArena;
        Path SSTablePath = basePath.resolve(Long.toString(tableId));
        this.memSegComp = memSegComp;
        Path summaryFilePath = SSTablePath.resolve("summary");
        Path indexFilePath = SSTablePath.resolve("index");
        Path dataFilePath = SSTablePath.resolve("data");
        if (rewrite) {
            write(memTable, summaryFilePath, indexFilePath, dataFilePath);
        } else {
            readOld(summaryFilePath, indexFilePath, dataFilePath);
        }

        summaryFile = summaryFile.asReadOnly();
        indexFile = indexFile.asReadOnly();
        dataFile = dataFile.asReadOnly();

        size = (summaryFile.byteSize() / Metadata.SIZE);
    }

    private void readOld(Path summaryFilePath, Path indexFilePath, Path dataFilePath) throws IOException {
        createIfNotExist(summaryFilePath);
        createIfNotExist(indexFilePath);
        createIfNotExist(dataFilePath);
        mapFiles(Files.size(summaryFilePath), Files.size(indexFilePath), Files.size(dataFilePath),
                summaryFilePath, indexFilePath, dataFilePath);
    }

    private void createIfNotExist(Path file) throws IOException {
        if (Files.notExists(file)) {
            Files.createDirectories(file.getParent());
            Files.createFile(file);
        }
    }

    private void prepareForWriting(Path file) throws IOException {
        if (Files.exists(file)) {
            Files.delete(file);
        }
        Files.createDirectories(file.getParent());
        Files.createFile(file);

    }

    private void mapFiles(long summarySize, long indexSize, long dataSize,
                          Path summaryFilePath, Path indexFilePath, Path dataFilePath) throws IOException {
        try (RandomAccessFile summaryRAFile = new RandomAccessFile(summaryFilePath.toString(), "rw");
             RandomAccessFile indexRAFile = new RandomAccessFile(indexFilePath.toString(), "rw");
             RandomAccessFile dataRAFile = new RandomAccessFile(dataFilePath.toString(), "rw")) {
            summaryFile = summaryRAFile.getChannel().map(FileChannel.MapMode.READ_WRITE,
                    0, summarySize, filesArena);
            indexFile = indexRAFile.getChannel().map(FileChannel.MapMode.READ_WRITE,
                    0, indexSize, filesArena);
            dataFile = dataRAFile.getChannel().map(FileChannel.MapMode.READ_WRITE,
                    0, dataSize, filesArena);
        }
    }

    private void writeEntry(Entry<MemorySegment> entry,
                            long summaryOffset, long indexOffset, long dataOffset) {
        MemorySegment.copy(entry.key(), 0, indexFile, indexOffset, entry.key().byteSize());
        if (entry.value() != null) {
            MemorySegment.copy(entry.value(), 0, dataFile, dataOffset, entry.value().byteSize());
        }
        Metadata.writeEntryMetadata(entry, summaryFile, summaryOffset, indexOffset, dataOffset);
    }

    // Sequentially writes every entity data in SStable keeping files data consistent
    public void write(NavigableMap<MemorySegment, Entry<MemorySegment>> memTable,
                      Path summaryFilePath, Path indexFilePath, Path dataFilePath) throws IOException {
        prepareForWriting(summaryFilePath);
        prepareForWriting(indexFilePath);
        prepareForWriting(dataFilePath);

        long indexSize = 0;
        long dataSize = 0;
        long summarySize;
        for (Entry<MemorySegment> entry : memTable.values()) {
            indexSize += entry.key().byteSize();
            dataSize += entry.value() == null ? 0 : entry.value().byteSize();
        }
        summarySize = memTable.size() * Metadata.SIZE;
        mapFiles(summarySize, indexSize, dataSize, summaryFilePath, indexFilePath, dataFilePath);

        long currentDataOffset = 0;
        long currentIndexOffset = 0;
        long currentSummaryOffset = 0;
        for (Entry<MemorySegment> entry : memTable.values()) {
            MemorySegment value = entry.value();
            value = value == null ? filesArena.allocate(0) : value;
            MemorySegment key = entry.key();
            writeEntry(entry, currentSummaryOffset, currentIndexOffset, currentDataOffset);
            currentDataOffset += value.byteSize();
            currentIndexOffset += key.byteSize();
            currentSummaryOffset += Metadata.SIZE;
        }
    }

    private Range readRange(MemorySegment segment, long offset) {
        return new Range(segment.get(ValueLayout.JAVA_LONG_UNALIGNED, offset),
                segment.get(ValueLayout.JAVA_LONG_UNALIGNED, offset + Long.BYTES));
    }

    // Binary search in summary and index files. Returns index of least record greater than key
    private long findByKey(MemorySegment key) {
        long left = 0;
        long right = size - 1;
        while (right - left > 2) {
            long middle = (right + left) / 2;
            Metadata currentEntryMetadata = new Metadata(middle);
            MemorySegment curKey = currentEntryMetadata.readKey();
            int compRes = memSegComp.compare(key, curKey);
            if (compRes <= 0) {
                right = middle;
            } else {
                left = middle;
            }
        }
        for (long i = left; i <= right; i++) {
            Metadata currentEntryMetadata = new Metadata(i);
            MemorySegment curKey = currentEntryMetadata.readKey();
            if (memSegComp.compare(curKey, key) >= 0) {
                return i;
            }
        }
        return -1;
    }

    private long findByKeyExact(MemorySegment key) {
        long goe = findByKey(key);
        if (goe == -1) return -1;
        if (memSegComp.compare(readEntry(goe).key(), key) == 0) return goe;
        return -1;
    }

    private Entry<MemorySegment> readEntry(long index) {
        Metadata metadata = new Metadata(index);
        MemorySegment key = metadata.readKey();
        MemorySegment value = metadata.readValue();
        return new BaseEntry<>(key, value);
    }

    public Entry<MemorySegment> find(MemorySegment key) throws IOException {
        long entryId = findByKeyExact(key);
        if (entryId == -1) return null;
        return readEntry(entryId);
    }

    public DatabaseIterator getRange(MemorySegment from, MemorySegment to) {
        return new SSTableIterator(from, to);
    }

    private class SSTableIterator implements DatabaseIterator {
        private long curItemIndex;
        private final MemorySegment maxKey;

        private Entry<MemorySegment> curEntry = null;

        public SSTableIterator(MemorySegment minKey, MemorySegment maxKey) {
            this.maxKey = maxKey;
            if (minKey == null) {
                this.curItemIndex = 0;
            } else {
                this.curItemIndex = findByKey(minKey);
            }
            if (curItemIndex != -1) {
                this.curEntry = readEntry(curItemIndex);
            } else {
                curItemIndex = Long.MAX_VALUE;
            }
        }

        @Override
        public boolean hasNext() {
            if (curItemIndex >= size) return false;
            return maxKey == null || memSegComp.compare(curEntry.key(), maxKey) < 0;
        }

        @Override
        public Entry<MemorySegment> next() {
            Entry<MemorySegment> result = curEntry;
            curItemIndex++;
            if (curItemIndex < size) {
                curEntry = readEntry(curItemIndex);
            }
            return result;
        }

        @Override
        public long getPriority() {
            return tableId;
        }
    }

    @Override
    public int compareTo(SSTable o) {
        return Long.compare(o.tableId, this.tableId);
    }

    // Describes offset and size of any data segment
    private static class Range {
        public long offset;
        public long length;

        public Range(long offset, long length) {
            this.offset = offset;
            this.length = length;
        }
    }

    private class Metadata {
        public final Range keyRange;
        public final Range valueRange;
        public final Boolean isDeletion;
        public final static long SIZE = Long.BYTES * 4 + 1;

        public Metadata(long index) {
            long base = index * Metadata.SIZE;
            keyRange = readRange(summaryFile, base);
            valueRange = readRange(summaryFile, base + 2 * Long.BYTES);
            isDeletion = summaryFile.get(ValueLayout.JAVA_BOOLEAN, base + 4 * Long.BYTES);
        }

        public MemorySegment readKey() {
            return indexFile.asSlice(keyRange.offset, keyRange.length);
        }

        public MemorySegment readValue() {
            return isDeletion ? null : dataFile.asSlice(valueRange.offset, valueRange.length);
        }

        public static void writeEntryMetadata(Entry<MemorySegment> entry, MemorySegment summaryFile,
                                              long sumOffset, long indexOffset, long dataOffset) {
            summaryFile.set(ValueLayout.JAVA_LONG_UNALIGNED,
                    sumOffset, indexOffset);
            summaryFile.set(ValueLayout.JAVA_LONG_UNALIGNED,
                    sumOffset + Long.BYTES, entry.key().byteSize());
            summaryFile.set(ValueLayout.JAVA_LONG_UNALIGNED,
                    sumOffset + 2 * Long.BYTES, dataOffset);
            summaryFile.set(ValueLayout.JAVA_BOOLEAN,
                    sumOffset + 4 * Long.BYTES, entry.value() == null);
            summaryFile.set(ValueLayout.JAVA_LONG_UNALIGNED,
                    sumOffset + 3 * Long.BYTES, entry.value() == null ? 0 : entry.value().byteSize());
        }

    }

}
