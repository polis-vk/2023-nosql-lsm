package ru.vk.itmo.kovalchukvladislav;

import ru.vk.itmo.Config;
import ru.vk.itmo.Entry;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.PriorityQueue;

public abstract class AbstractBasedOnSSTableDao<D, E extends Entry<D>> extends AbstractInMemoryDao<D, E> {
    //  ===================================
    //  Constants
    //  ===================================
    private static final ValueLayout.OfLong LONG_LAYOUT = ValueLayout.JAVA_LONG_UNALIGNED;
    private static final String OFFSETS_FILENAME_PREFIX = "offsets_";
    private static final String METADATA_FILENAME = "metadata";
    private static final String DB_FILENAME_PREFIX = "db_";
    private static final int SIZE_LENGTH = Long.BYTES;
    private static final long VALUE_IS_NULL_SIZE = -1;

    //  ===================================
    //  Variables
    //  ===================================

    private final Path basePath;
    private final Path metadataPath;
    private final Arena arena = Arena.ofShared();
    private final MemorySegmentSerializer<D, E> serializer;

    //  ===================================
    //  Storages
    //  ===================================

    private final int storagesCount;
    private final List<FileChannel> dbFileChannels;
    private final List<FileChannel> offsetChannels;
    private final List<MemorySegment> dbMappedSegments;
    private final List<MemorySegment> offsetMappedSegments;

    protected AbstractBasedOnSSTableDao(Config config,
                                   Comparator<? super D> comparator,
                                   MemorySegmentSerializer<D, E> serializer) throws IOException {
        super(comparator);
        this.serializer = serializer;
        this.basePath = Objects.requireNonNull(config.basePath());

        if (!Files.exists(basePath)) {
            Files.createDirectory(basePath);
        }
        this.metadataPath = basePath.resolve(METADATA_FILENAME);

        this.storagesCount = getCountFromMetadataOrCreate();
        this.dbFileChannels = new ArrayList<>(storagesCount);
        this.offsetChannels = new ArrayList<>(storagesCount);
        this.dbMappedSegments = new ArrayList<>(storagesCount);
        this.offsetMappedSegments = new ArrayList<>(storagesCount);

        for (int i = 0; i < storagesCount; i++) {
            readFileAndMapToSegment(DB_FILENAME_PREFIX, i, dbFileChannels, dbMappedSegments);
            readFileAndMapToSegment(OFFSETS_FILENAME_PREFIX, i, offsetChannels, offsetMappedSegments);
        }
    }

    //  ===================================
    //  Restoring state
    //  ===================================
    private int getCountFromMetadataOrCreate() throws IOException {
        if (!Files.exists(metadataPath)) {
            Files.writeString(metadataPath, "0", StandardOpenOption.WRITE, StandardOpenOption.CREATE);
            return 0;
        }
        return Integer.parseInt(Files.readString(metadataPath));
    }

    private void readFileAndMapToSegment(String filenamePrefix, int index,
                                         List<FileChannel> channels,
                                         List<MemorySegment> segments) throws IOException {
        Path path = basePath.resolve(filenamePrefix + index);
        FileChannel channel = FileChannel.open(path, StandardOpenOption.READ);
        MemorySegment segment = channel.map(FileChannel.MapMode.READ_ONLY, 0, Files.size(path), arena);
        channels.add(channel);
        segments.add(segment);
    }

    //  ===================================
    //  Finding in storage
    //  ===================================
    @Override
    public Iterator<E> get(D from, D to) {
        Iterator<E> inMemotyIterator = super.get(from, to);
        List<StorageIterator> storageIterators = new ArrayList<>(storagesCount);
        for (int i = 0; i < storagesCount; i++) {
            storageIterators.add(new StorageIterator(dbMappedSegments.get(i), offsetMappedSegments.get(i), from, to));
        }
        return new DaoIterator(inMemotyIterator, storageIterators);
    }

    @Override
    public E get(D key) {
        E e = dao.get(key);
        if (e != null) {
            return e.value() == null ? null : e;
        }
        E fromFile = findInStorages(key);
        return (fromFile == null || fromFile.value() == null) ? null : fromFile;
    }

    private E findInStorages(D key) {
        for (int i = storagesCount - 1; i >= 0; i--) {
            MemorySegment storage = dbMappedSegments.get(i);
            MemorySegment offsets = offsetMappedSegments.get(i);

            long lowerBoundOffset = findLowerBoundOffset(key, storage, offsets);
            if (lowerBoundOffset == -1) {
                continue;
            }
            D lowerBoundKey = readValue(storage, lowerBoundOffset);
            if (comparator.compare(lowerBoundKey, key) == 0) {
                D value = readValue(storage, lowerBoundOffset + SIZE_LENGTH + serializer.size(lowerBoundKey));
                return serializer.createEntry(lowerBoundKey, value);
            }
        }
        return null;
    }

    /**
     * Returns the greater offset that storage.get(LONG_LAYOUT, offset).key() <= key<br>
     * -1 otherwise
     */
    private long findLowerBoundOffset(D key, MemorySegment storage, MemorySegment offsets) {
        long entriesCount = offsets.byteSize() / SIZE_LENGTH;
        long left = -1;
        long right = entriesCount;

        while (left + 1 < right) {
            long middle = left + (right - left) / 2;
            long middleOffset = offsets.getAtIndex(LONG_LAYOUT, middle);
            D middleKey = readValue(storage, middleOffset);

            if (comparator.compare(middleKey, key) <= 0) {
                left = middle;
            } else {
                right = middle;
            }
        }
        return left == -1 ? -1 : offsets.getAtIndex(LONG_LAYOUT, left);
    }

    //  ===================================
    //  Reading values
    //  ===================================

    private D readValue(MemorySegment memorySegment, long offset) {
        long size = memorySegment.get(LONG_LAYOUT, offset);
        if (size == VALUE_IS_NULL_SIZE) {
            return null;
        }
        MemorySegment valueSegment = memorySegment.asSlice(offset + SIZE_LENGTH, size);
        return serializer.toValue(valueSegment);
    }

    // Return new offset
    private long writeValue(D value, MemorySegment memorySegment, long offset) {
        MemorySegment valueSegment = serializer.fromValue(value);
        if (valueSegment == null) {
            memorySegment.set(LONG_LAYOUT, offset, VALUE_IS_NULL_SIZE);
            return offset + SIZE_LENGTH;
        }
        long size = valueSegment.byteSize();
        memorySegment.set(LONG_LAYOUT, offset, size);
        MemorySegment.copy(valueSegment, 0, memorySegment, offset + SIZE_LENGTH, size);
        return offset + SIZE_LENGTH + size;
    }

    //  ===================================
    //  Writing offsets and data
    //  ===================================

    private void writeData() throws IOException {
        Path dbPath = basePath.resolve(DB_FILENAME_PREFIX + storagesCount);
        Path offsetsPath = basePath.resolve(OFFSETS_FILENAME_PREFIX + storagesCount);

        OpenOption[] options = new OpenOption[] {
                StandardOpenOption.READ,
                StandardOpenOption.WRITE,
                StandardOpenOption.TRUNCATE_EXISTING,
                StandardOpenOption.CREATE
        };

        try (FileChannel db = FileChannel.open(dbPath, options);
             FileChannel offsets = FileChannel.open(offsetsPath, options);
             Arena confined = Arena.ofConfined()) {

            long dbSize = getDAOBytesSize();
            long offsetsSize = (long) dao.size() * Long.BYTES;
            MemorySegment fileSegment = db.map(FileChannel.MapMode.READ_WRITE, 0, dbSize, confined);
            MemorySegment offsetsSegment = offsets.map(FileChannel.MapMode.READ_WRITE, 0, offsetsSize, confined);

            int i = 0;
            long offset = 0;
            for (E entry : dao.values()) {
                offsetsSegment.setAtIndex(LONG_LAYOUT, i, offset);
                i += 1;

                offset = writeValue(entry.key(), fileSegment, offset);
                offset = writeValue(entry.value(), fileSegment, offset);
            }
            fileSegment.load();
            offsetsSegment.load();
        }
    }

    private long getDAOBytesSize() {
        long size = 0;
        for (E entry : dao.values()) {
            size += getEntryBytesSize(entry);
        }
        return size;
    }

    private long getEntryBytesSize(E entry) {
        return 2 * SIZE_LENGTH + serializer.size(entry.key()) + serializer.size(entry.value());
    }

    //  ===================================
    //  Close and flush
    //  ===================================

    @Override
    public synchronized void flush() throws IOException {
        if (!dao.isEmpty()) {
            writeData();
            Files.writeString(metadataPath, String.valueOf(storagesCount + 1));
        }
    }

    @Override
    public synchronized void close() throws IOException {
        if (arena.scope().isAlive()) {
            arena.close();
        }
        flush();
        closeChannels(dbFileChannels);
        closeChannels(offsetChannels);
    }

    private void closeChannels(List<FileChannel> channels) throws IOException {
        for (FileChannel channel : channels) {
            if (channel.isOpen()) {
                channel.close();
            }
        }
    }

    //  ===================================
    //  Iterators
    //  ===================================

    private class DaoIterator implements Iterator<E> {
        private static final Integer IN_MEMORY_ITERATOR_ID = Integer.MAX_VALUE;
        private final Iterator<E> inMemoryIterator;
        private final List<StorageIterator> storageIterators;
        private final PriorityQueue<IndexedEntry> queue;

        public DaoIterator(Iterator<E> inMemoryIterator, List<StorageIterator> storageIterators) {
            this.inMemoryIterator = inMemoryIterator;
            this.storageIterators = storageIterators;
            this.queue = new PriorityQueue<>(1 + storageIterators.size());

            addEntryByIteratorIdSafe(IN_MEMORY_ITERATOR_ID);
            for (int i = 0; i < storageIterators.size(); i++) {
                addEntryByIteratorIdSafe(i);
            }
            cleanByNull();
        }

        @Override
        public boolean hasNext() {
            return !queue.isEmpty();
        }

        @Override
        public E next() {
            if (queue.isEmpty()) {
                throw new NoSuchElementException();
            }
            IndexedEntry minElement = queue.peek();
            E minEntry = minElement.entry;
            cleanByKey(minElement.entry.key());
            cleanByNull();
            return minEntry;
        }

        private void cleanByKey(D key) {
            while (!queue.isEmpty() && comparator.compare(queue.peek().entry.key(), key) == 0) {
                IndexedEntry removedEntry = queue.remove();
                int iteratorId = removedEntry.iteratorId;
                addEntryByIteratorIdSafe(iteratorId);
            }
        }

        private void cleanByNull() {
            while (!queue.isEmpty()) {
                E entry = queue.peek().entry;
                if (entry.value() != null) {
                    break;
                }
                cleanByKey(entry.key());
            }
        }

        private void addEntryByIteratorIdSafe(int iteratorId) {
            Iterator<E> iteratorById = getIteratorById(iteratorId);
            if (iteratorById.hasNext()) {
                E next = iteratorById.next();
                queue.add(new IndexedEntry(iteratorId, next));
            }
        }

        private Iterator<E> getIteratorById(int id) {
            if (id == IN_MEMORY_ITERATOR_ID) {
                return inMemoryIterator;
            }
            return storageIterators.get(id);
        }
    }

    private class IndexedEntry implements Comparable<IndexedEntry> {
        final int iteratorId;
        final E entry;

        public IndexedEntry(int iteratorId, E entry) {
            this.iteratorId = iteratorId;
            this.entry = entry;
        }

        @Override
        public int compareTo(IndexedEntry other) {
            int compared = comparator.compare(entry.key(), other.entry.key());
            if (compared != 0) {
                return compared;
            }
            return -Integer.compare(iteratorId, other.iteratorId);
        }
    }

    private class StorageIterator implements Iterator<E> {
        private final MemorySegment storageSegment;
        private final long end;
        private long start;

        public StorageIterator(MemorySegment storageSegment, MemorySegment offsetsSegment, D from, D to) {
            this.storageSegment = storageSegment;

            if (offsetsSegment.byteSize() == 0) {
                this.start = -1;
                this.end = -1;
            } else {
                this.start = calculateStartPosition(offsetsSegment, from);
                this.end = calculateEndPosition(offsetsSegment, to);
            }
        }

        private long calculateStartPosition(MemorySegment offsetsSegment, D from) {
            if (from == null) {
                return getFirstOffset(offsetsSegment);
            }
            long lowerBoundOffset = findLowerBoundOffset(from, storageSegment, offsetsSegment);
            if (lowerBoundOffset == -1) {
                // from the smallest element and doesn't exist
                return getFirstOffset(offsetsSegment);
            } else {
                // storage[lowerBoundOffset] <= from, we need >= from only
                return moveOffsetIfFirstKeyAreNotEqual(from, lowerBoundOffset);
            }
        }

        private long calculateEndPosition(MemorySegment offsetsSegment, D to) {
            if (to == null) {
                return getEndOffset();
            }
            long lowerBoundOffset = findLowerBoundOffset(to, storageSegment, offsetsSegment);
            if (lowerBoundOffset == -1) {
                // to the smallest element and doesn't exist
                return getFirstOffset(offsetsSegment);
            }
            // storage[lowerBoundOffset] <= to, we need >= to only
            return moveOffsetIfFirstKeyAreNotEqual(to, lowerBoundOffset);
        }

        private long getFirstOffset(MemorySegment offsetsSegment) {
            return offsetsSegment.getAtIndex(LONG_LAYOUT, 0);
        }

        private long getEndOffset() {
            return storageSegment.byteSize();
        }

        private long moveOffsetIfFirstKeyAreNotEqual(D from, long lowerBoundOffset) {
            long offset = lowerBoundOffset;
            D lowerBoundKey = readValue(storageSegment, offset);
            if (comparator.compare(lowerBoundKey, from) != 0) {
                offset += SIZE_LENGTH;
                offset += serializer.size(lowerBoundKey);
                D lowerBoundValue = readValue(storageSegment, offset);
                offset += SIZE_LENGTH;
                offset += serializer.size(lowerBoundValue);
            }
            return offset;
        }

        @Override
        public boolean hasNext() {
            return start < end;
        }

        @Override
        public E next() {
            D key = readValue(storageSegment, start);
            start += SIZE_LENGTH;
            start += serializer.size(key);
            D value = readValue(storageSegment, start);
            start += SIZE_LENGTH;
            start += serializer.size(value);
            return serializer.createEntry(key, value);
        }
    }
}
