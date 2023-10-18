package ru.vk.itmo.kovalchukvladislav;

import ru.vk.itmo.Config;
import ru.vk.itmo.Dao;
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
import java.util.Comparator;
import java.util.Iterator;
import java.util.Objects;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * Saves state (two files) when closed in <code>config.basePath()</code><br> directory.
 *
 * <p>State contains two files:
 * <li>
 * <tt>db</tt> with sorted by key entries
 * </li>
 * <li>
 * <tt>offsets</tt> with keys offsets at <tt>db</tt>(in bytes)
 * </li>
 *
 * <p>This allows use binary search after <tt>db</tt> reading
 */
public abstract class AbstractInMemoryDao<D, E extends Entry<D>> implements Dao<D, E> {
    private static final ValueLayout.OfLong LONG_LAYOUT = ValueLayout.JAVA_LONG_UNALIGNED;
    private static final String DB_FILENAME_PREFIX = "db_";
    private static final String OFFSETS_FILENAME_PREFIX = "offsets_";
    private static final String METADATA_FILENAME = "metadata";
    private static final int SIZE_LENGTH = Long.BYTES;

    private final Path basePath;
    private final Path metadataPath;
    private final Arena arena = Arena.ofShared();
    private final Comparator<? super D> comparator;
    private final ConcurrentNavigableMap<D, E> dao;
    private final MemorySegmentSerializer<D, E> serializer;

    private final int storagesCount;
    private final FileChannel[] dbFileChannels;
    private final FileChannel[] offsetChannels;
    private final MemorySegment[] dbMappedSegments;
    private final MemorySegment[] offsetMappedSegments;

    protected AbstractInMemoryDao(Config config,
                                  Comparator<? super D> comparator,
                                  MemorySegmentSerializer<D, E> serializer) throws IOException {
        this.comparator = comparator;
        this.serializer = serializer;
        this.dao = new ConcurrentSkipListMap<>(comparator);
        this.basePath = Objects.requireNonNull(config.basePath());
        Files.createDirectories(basePath);
        this.metadataPath = basePath.resolve(METADATA_FILENAME);

        this.storagesCount = getCountFromMetadataOrCreate();
        this.dbFileChannels = new FileChannel[storagesCount];
        this.offsetChannels = new FileChannel[storagesCount];
        this.dbMappedSegments = new MemorySegment[storagesCount];
        this.offsetMappedSegments = new MemorySegment[storagesCount];

        for (int i = 0; i < storagesCount; i++) {
            readFileAndMapToSegment(DB_FILENAME_PREFIX, i, dbFileChannels, dbMappedSegments);
            readFileAndMapToSegment(OFFSETS_FILENAME_PREFIX, i, offsetChannels, offsetMappedSegments);
        }
    }

    private int getCountFromMetadataOrCreate() throws IOException {
        if (!Files.exists(metadataPath)) {
            Files.writeString(metadataPath, "0", StandardOpenOption.WRITE, StandardOpenOption.CREATE);
            return 0;
        }
        return Integer.parseInt(Files.readString(metadataPath));
    }

    private void readFileAndMapToSegment(String filenamePrefix, int index,
                                         FileChannel[] dstChannel,
                                         MemorySegment[] dstSegment) throws IOException {
        Path path = basePath.resolve(filenamePrefix + index);

        FileChannel fileChannel = FileChannel.open(path, StandardOpenOption.READ);
        MemorySegment mappedSegment = fileChannel.map(FileChannel.MapMode.READ_ONLY, 0, Files.size(path), arena);

        dstChannel[index] = fileChannel;
        dstSegment[index] = mappedSegment;
    }

    @Override
    public Iterator<E> get(D from, D to) {
        if (from == null && to == null) {
            return all();
        } else if (from == null) {
            return allTo(to);
        } else if (to == null) {
            return allFromUnsafe(from);
        }
        return dao.subMap(from, to).values().iterator();
    }

    @Override
    public E get(D key) {
        E e = dao.get(key);
        if (e != null) {
            return e.value() == null ? null : e;
        }
        for (int i = storagesCount - 1; i >= 0; i--) {
            E fromFile = findInStorage(key, i);
            if (fromFile != null) {
                return fromFile.value() == null ? null : fromFile;
            }
        }
        return null;
    }

    @Override
    public void upsert(E entry) {
        dao.put(entry.key(), entry);
    }

    @Override
    public Iterator<E> allFrom(D from) {
        return from == null ? all() : allFromUnsafe(from);
    }

    /**
     * Doesn't check the argument for null. Should be called only if there was a check before
     *
     * @param from NotNull lower bound of range (inclusive)
     * @return entries with key >= from
     */
    private Iterator<E> allFromUnsafe(D from) {
        return dao.tailMap(from).values().iterator();
    }

    @Override
    public Iterator<E> allTo(D to) {
        return to == null ? all() : allToUnsafe(to);
    }

    /**
     * Doesn't check the argument for null. Should be called only if there was a check before
     *
     * @param to NotNull upper bound of range (exclusive)
     * @return entries with key < to
     */
    private Iterator<E> allToUnsafe(D to) {
        return dao.headMap(to).values().iterator();
    }

    @Override
    public Iterator<E> all() {
        return dao.values().iterator();
    }

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

    private void closeChannels(FileChannel[] channels) throws IOException {
        for (FileChannel channel : channels) {
            if (channel.isOpen()) {
                channel.close();
            }
        }
    }

    //  ===================================
    //  Reading values
    //  ===================================

    private D readValue(MemorySegment memorySegment, long offset) {
        long size = memorySegment.get(LONG_LAYOUT, offset);
        if (size == 0) {
            return null;
        }
        MemorySegment valueSegment = memorySegment.asSlice(offset + SIZE_LENGTH, size);
        return serializer.toValue(valueSegment);
    }

    // Return new offset
    private long writeValue(D value, MemorySegment memorySegment, long offset) {
        MemorySegment valueSegment = serializer.fromValue(value);
        long size = valueSegment.byteSize();
        memorySegment.set(LONG_LAYOUT, offset, size);
        if (size != 0) {
            MemorySegment.copy(valueSegment, 0, memorySegment, offset + SIZE_LENGTH, size);
        }
        return offset + SIZE_LENGTH + size;
    }

    private E findInStorage(D key, int index) {
        MemorySegment storage = dbMappedSegments[index];
        MemorySegment offsets = offsetMappedSegments[index];

        long upperBoundOffset = findUpperBoundOffset(key, storage, offsets);
        if (upperBoundOffset == -1) {
            return null;
        }
        D upperBoundKey = readValue(storage, upperBoundOffset);
        if (comparator.compare(upperBoundKey, key) == 0) {
            D value = readValue(storage, upperBoundOffset + SIZE_LENGTH + serializer.size(upperBoundKey));
            return serializer.createEntry(upperBoundKey, value);
        }
        return null;
    }

    /**
     * Returns offset that storage.get(LONG_LAYOUT, offset).key() >= key<br>
     * -1 otherwise
     */
    private long findUpperBoundOffset(D key, MemorySegment storage, MemorySegment offsets) {
        long entriesCount = offsets.byteSize() / SIZE_LENGTH;
        long left = -1;
        long right = entriesCount;

        while (left + 1 < right) {
            long middle = left + (right - left) / 2;
            long middleOffset = offsets.getAtIndex(LONG_LAYOUT, middle);
            D middleKey = readValue(storage, middleOffset);

            if (comparator.compare(middleKey, key) < 0) {
                left = middle;
            } else {
                right = middle;
            }
        }
        if (right == entriesCount) {
            return -1;
        }
        return offsets.getAtIndex(LONG_LAYOUT, right);
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
             Arena arena = Arena.ofConfined()) {

            long dbSize = getDAOBytesSize();
            long offsetsSize = (long) dao.size() * Long.BYTES;
            MemorySegment fileSegment = db.map(FileChannel.MapMode.READ_WRITE, 0, dbSize, arena);
            MemorySegment offsetsSegment = offsets.map(FileChannel.MapMode.READ_WRITE, 0, offsetsSize, arena);

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
}
