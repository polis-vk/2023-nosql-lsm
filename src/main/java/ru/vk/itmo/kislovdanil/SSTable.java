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
    private final Arena filesArena = Arena.ofAuto();
    private final long tableId;

    private final long size;

    public SSTable(Path basePath, Comparator<MemorySegment> memSegComp, long tableId,
                   NavigableMap<MemorySegment, Entry<MemorySegment>> memTable, boolean rewrite) throws IOException {
        this.tableId = tableId;
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

        size = (summaryFile.byteSize() / (2 * Long.BYTES));
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

    private void writeEntry(MemorySegment key, MemorySegment value,
                            long currentSummaryOffset, long currentIndexOffset, long currentDataOffset) {
        MemorySegment.copy(value, 0, dataFile, currentDataOffset, value.byteSize());
        MemorySegment.copy(key, 0, indexFile, currentIndexOffset, key.byteSize());
        indexFile.set(ValueLayout.JAVA_LONG_UNALIGNED
                currentIndexOffset + key.byteSize(), currentDataOffset);
        indexFile.set(ValueLayout.JAVA_LONG_UNALIGNED,
                currentIndexOffset + key.byteSize() + Long.BYTES, value.byteSize());
        summaryFile.set(ValueLayout.JAVA_LONG_UNALIGNED,
                currentSummaryOffset, currentIndexOffset);
        summaryFile.set(ValueLayout.JAVA_LONG_UNALIGNED,
                currentSummaryOffset + Long.BYTES, key.byteSize());
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
            dataSize += entry.value().byteSize();
        }
        summarySize = 2L * memTable.size() * Long.BYTES;
        indexSize += summarySize;

        mapFiles(summarySize, indexSize, dataSize, summaryFilePath, indexFilePath, dataFilePath);

        long currentDataOffset = 0;
        long currentIndexOffset = 0;
        long currentSummaryOffset = 0;
        for (Entry<MemorySegment> entry : memTable.values()) {
            MemorySegment value = entry.value();
            MemorySegment key = entry.key();
            writeEntry(key, value, currentSummaryOffset, currentIndexOffset, currentDataOffset);
            currentDataOffset += value.byteSize();
            currentIndexOffset += key.byteSize() + 2 * Long.BYTES;
            currentSummaryOffset += 2 * Long.BYTES;
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
            Range indexRange = readRange(summaryFile, middle * Long.BYTES * 2);
            MemorySegment curKey = indexFile.asSlice(indexRange.offset, indexRange.length);
            int compRes = memSegComp.compare(key, curKey);
            if (compRes <= 0) {
                right = middle;
            } else {
                left = middle;
            }
        }
        for (long i = left; i <= right; i++) {
            Range indexRange = readRange(summaryFile, i * Long.BYTES * 2);
            MemorySegment curKey = indexFile.asSlice(indexRange.offset, indexRange.length);
            if (memSegComp.compare(key, curKey) <= 0) {
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
        Range indexRange = readRange(summaryFile, index * 2 * Long.BYTES);
        MemorySegment key = indexFile.asSlice(indexRange.offset, indexRange.length);
        Range dataRange = readRange(indexFile, indexRange.offset + indexRange.length);
        MemorySegment value = dataFile.asSlice(dataRange.offset, dataRange.length);
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

        private Entry<MemorySegment> curEntry;

        public SSTableIterator(MemorySegment minKey, MemorySegment maxKey) {
            this.maxKey = maxKey;
            if (minKey == null) {
                this.curItemIndex = 0;
            } else {
                this.curItemIndex = findByKey(minKey);
            }
            this.curEntry = readEntry(curItemIndex);
        }

        @Override
        public boolean hasNext() {
            if (curItemIndex >= size) return false;
            return maxKey != null && memSegComp.compare(curEntry.key(), maxKey) < 0;
        }

        @Override
        public Entry<MemorySegment> next() {
            Entry<MemorySegment> result = curEntry;
            curEntry = readEntry(++curItemIndex);
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

}
