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
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class SSTable implements Comparable<SSTable>, Iterable<Entry<MemorySegment>> {
    // Contains offset and size for every key and every value in index file
    private MemorySegment summaryFile;
    // Contains keys
    private MemorySegment indexFile;
    // Contains values
    private MemorySegment dataFile;
    private final Comparator<MemorySegment> memSegComp;
    private final Arena filesArena = Arena.ofAuto();
    private final long tableId;
    private final Path ssTablePath;

    private final long size;

    /* In case deletion while compaction of this table field would link to table with compacted data.
    Necessary for iterators created before compaction. */
    private SSTable compactedTo = null;
    // Gives a guarantee that SSTable files wouldn't be deleted while reading
    private final ReadWriteLock readWriteLock = new ReentrantReadWriteLock();

    public long getTableId() {
        return tableId;
    }

    public SSTable(Path basePath, Comparator<MemorySegment> memSegComp, long tableId,
                   Iterator<Entry<MemorySegment>> entriesContainer,
                   boolean rewrite) throws IOException {
        this.tableId = tableId;
        ssTablePath = basePath.resolve(Long.toString(tableId));
        this.memSegComp = memSegComp;
        Path summaryFilePath = ssTablePath.resolve("summary");
        Path indexFilePath = ssTablePath.resolve("index");
        Path dataFilePath = ssTablePath.resolve("data");
        if (rewrite) {
            write(entriesContainer, summaryFilePath, indexFilePath, dataFilePath);
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
        summaryFile = mapFile(Files.size(summaryFilePath), summaryFilePath);
        indexFile = mapFile(Files.size(indexFilePath), indexFilePath);
        dataFile = mapFile(Files.size(dataFilePath), dataFilePath);
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

    private MemorySegment mapFile(long size, Path filePath) throws IOException {
        try (RandomAccessFile raFile = new RandomAccessFile(filePath.toString(), "rw")) {
            return raFile.getChannel().map(FileChannel.MapMode.READ_WRITE, 0, size, filesArena);
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

    private long[] getFilesSize(Iterable<Entry<MemorySegment>> entriesContainer) {
        long indexSize = 0;
        long dataSize = 0;
        long summarySize = 0;
        for (Entry<MemorySegment> entry : entriesContainer) {
            indexSize += entry.key().byteSize();
            dataSize += entry.value() == null ? 0 : entry.value().byteSize();
            summarySize += Metadata.SIZE;
        }
        return new long[]{summarySize, indexSize, dataSize};
    }

    // Sequentially writes every entity data in SStable keeping files data consistent
    private void write(Iterator<Entry<MemorySegment>> entryIterator,
                       Path summaryFilePath, Path indexFilePath, Path dataFilePath) throws IOException {
        prepareForWriting(summaryFilePath);
        prepareForWriting(indexFilePath);
        prepareForWriting(dataFilePath);

        List<Entry<MemorySegment>> entries = new ArrayList<>();
        while (entryIterator.hasNext()) {
            entries.add(entryIterator.next());
        }

        long[] filesSize = getFilesSize(entries);

        summaryFile = mapFile(filesSize[0], summaryFilePath);
        indexFile = mapFile(filesSize[1], indexFilePath);
        dataFile = mapFile(filesSize[2], dataFilePath);

        long currentSummaryOffset = 0;
        long currentIndexOffset = 0;
        long currentDataOffset = 0;
        for (Entry<MemorySegment> entry : entries) {
            MemorySegment value = entry.value();
            value = value == null ? filesArena.allocate(0) : value;
            MemorySegment key = entry.key();
            writeEntry(entry, currentSummaryOffset, currentIndexOffset, currentDataOffset);
            currentDataOffset += value.byteSize();
            currentIndexOffset += key.byteSize();
            currentSummaryOffset += Metadata.SIZE;
        }
    }

    // Deletes all SSTable files from disk. Don't use object after invocation of this method!
    public void deleteFromDisk(SSTable compactedTo) throws IOException {
        readWriteLock.writeLock().lock();
        try {
            this.compactedTo = compactedTo;
            Files.delete(ssTablePath.resolve("summary"));
            Files.delete(ssTablePath.resolve("index"));
            Files.delete(ssTablePath.resolve("data"));
            Files.delete(ssTablePath);
        } finally {
            readWriteLock.writeLock().unlock();
        }
    }

    private Range readRange(MemorySegment segment, long offset) {
        return new Range(segment.get(ValueLayout.JAVA_LONG_UNALIGNED, offset),
                segment.get(ValueLayout.JAVA_LONG_UNALIGNED, offset + Long.BYTES));
    }

    /* Binary search in summary and index files. Returns index of least record greater than key or equal.
    Returns -1 if no such key */
    private long findByKey(MemorySegment key) {
        long left = -1; // Always less than key
        long right = size; // Always greater or equal than key
        while (right - left > 1) {
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
        return right == size ? -1 : right; // If right == size, then key is bigger than any SSTable key
    }

    private long findByKeyExact(MemorySegment key) {
        long goe = findByKey(key);
        if (goe == -1 || memSegComp.compare(readEntry(goe).key(), key) != 0) return -1;
        return goe;
    }

    private Entry<MemorySegment> readEntry(long index) {
        Metadata metadata = new Metadata(index);
        MemorySegment key = metadata.readKey();
        MemorySegment value = metadata.readValue();
        return new BaseEntry<>(key, value);
    }

    public Entry<MemorySegment> find(MemorySegment key) throws IOException {
        if (compactedTo != null) {
            return compactedTo.find(key);
        }
        readWriteLock.readLock().lock();
        try {
            long entryId = findByKeyExact(key);
            if (entryId == -1) return null;
            return readEntry(entryId);
        } finally {
            readWriteLock.readLock().unlock();
        }
    }

    public DatabaseIterator getRange(MemorySegment from, MemorySegment to) {
        return new SSTableIterator(from, to, this);
    }

    public DatabaseIterator getRange() {
        return getRange(null, null);
    }

    @Override
    public Iterator<Entry<MemorySegment>> iterator() {
        return getRange();
    }

    private class SSTableIterator implements DatabaseIterator {
        private long curItemIndex;
        private final MemorySegment maxKey;

        private Entry<MemorySegment> curEntry;
        private SSTable table;

        public SSTableIterator(MemorySegment minKey, MemorySegment maxKey, SSTable table) {
            this.table = table;
            this.maxKey = maxKey;
            if (table.size == 0) return;
            this.table.readWriteLock.readLock().lock();
            try {
                if (minKey == null) {
                    this.curItemIndex = 0;
                } else {
                    this.curItemIndex = this.table.findByKey(minKey);
                }
                if (curItemIndex == -1) {
                    curItemIndex = Long.MAX_VALUE;
                } else {
                    this.curEntry = this.table.readEntry(curItemIndex);
                }
            } finally {
                this.table.readWriteLock.readLock().unlock();
            }
        }

        private void updateTable() {
            if (this.table.compactedTo == null) return;
            while (this.table.compactedTo != null) {
                this.table = this.table.compactedTo;
            }
            this.table.readWriteLock.readLock().lock();
            try {
                this.curItemIndex = this.table.findByKey(curEntry.key());
            } finally {
                this.table.readWriteLock.readLock().unlock();
            }
        }

        @Override
        public boolean hasNext() {
            updateTable();
            if (curItemIndex >= this.table.size) return false;
            return maxKey == null || memSegComp.compare(curEntry.key(), maxKey) < 0;
        }

        @Override
        public Entry<MemorySegment> next() {
            Entry<MemorySegment> result = curEntry;
            updateTable();
            this.table.readWriteLock.readLock().lock();
            try {
                curItemIndex++;
                if (curItemIndex < size) {
                    curEntry = readEntry(curItemIndex);
                }
                return result;
            } finally {
                this.table.readWriteLock.readLock().unlock();
            }
        }

        @Override
        public long getPriority() {
            return tableId;
        }
    }

    // The less the ID, the less the table
    @Override
    public int compareTo(SSTable o) {
        return Long.compare(this.tableId, o.tableId);
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
        private final Range keyRange;
        private final Range valueRange;
        private final Boolean isDeletion;
        public static final long SIZE = Long.BYTES * 4 + 1;

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
