package ru.vk.itmo.kovalchukvladislav;

import ru.vk.itmo.Config;
import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.UncheckedIOException;
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
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.logging.Logger;

/**
 * Saves state (two files) when closed in <code>config.basePath()</code><br> directory.
 *
 * <p>Directory contains two files:
 * <li>
 * <tt>db</tt> with sorted by key entries
 * </li>
 * <li>
 * <tt>offsets</tt> with keys offsets at <tt>db</tt>(in bytes)
 * </li>
 *
 * <p>This allows use binary search after <tt>db</tt> reading (usually file much less)
 */
public abstract class AbstractInMemoryDao<D, E extends Entry<D>> implements Dao<D, E> {
    private static final String DB_FILENAME = "db";
    private static final int SIZE_LENGTH = Long.BYTES;
    private static final String OFFSETS_FILENAME = "offsets";
    private static final ValueLayout.OfLong LONG_LAYOUT = ValueLayout.JAVA_LONG_UNALIGNED;

    private final Path basePath;
    private volatile boolean closed;
    private volatile MemorySegment storageOffsets;
    private final Comparator<? super D> comparator;
    private final ConcurrentNavigableMap<D, E> dao;
    private final MemorySegmentSerializer<D, E> serializer;
    private final Logger logger = Logger.getLogger("InMemoryDao");

    protected AbstractInMemoryDao(Config config,
                                  Comparator<? super D> comparator,
                                  MemorySegmentSerializer<D, E> serializer) {
        this.comparator = comparator;
        this.serializer = serializer;
        this.dao = new ConcurrentSkipListMap<>(comparator);
        this.basePath = config.basePath();
    }

    @Override
    public Iterator<E> get(D from, D to) {
        if (from == null && to == null) {
            return all();
        } else if (from == null) {
            return allToUnsafe(to);
        } else if (to == null) {
            return allFromUnsafe(from);
        }
        return dao.subMap(from, to).values().iterator();
    }

    @Override
    public E get(D key) {
        E e = dao.get(key);
        if (e != null) {
            return e;
        }
        E fromFile = findInStorage(key);
        if (fromFile == null) {
            return null;
        }
        E previousValue = dao.putIfAbsent(key, fromFile);
        if (previousValue != null) {
            // If new value was putted while we were looking for in storage, just return it
            // Maybe should return previousValue, as value which was stored when method called
            // But this is concurrency, there are no guarantees
            return previousValue;
        }
        return fromFile;
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
        writeData();
        this.storageOffsets = null;
    }

    @Override
    public synchronized void close() throws IOException {
        if (!closed) {
            flush();
            closed = true;
        }
    }

    //  ===================================
    //  Reading values
    //  ===================================

    private D readValue(MemorySegment memorySegment, long offset) {
        long size = memorySegment.get(LONG_LAYOUT, offset);
        MemorySegment valueSegment = memorySegment.asSlice(offset + SIZE_LENGTH, size);
        return serializer.toValue(valueSegment);
    }

    // Return new offset
    private long writeValue(D value, MemorySegment memorySegment, long offset) {
        MemorySegment valueSegment = serializer.fromValue(value);
        long size = valueSegment.byteSize();

        memorySegment.set(LONG_LAYOUT, offset, size);
        MemorySegment.copy(valueSegment, 0, memorySegment, offset + SIZE_LENGTH, size);
        return offset + SIZE_LENGTH + size;
    }

    //  ===================================
    //  Reading offsets and data
    //  ===================================

    /**
     * Read offsets from file.<br>
     * If file doesn't exist will be <code>MemorySegment.NULL</code>
     */
    private synchronized void readOffsets() {
        if (storageOffsets != null) {
            return;
        }
        File offsetsFile = basePath.resolve(OFFSETS_FILENAME).toFile();
        if (!offsetsFile.exists()) {
            logger.warning(() ->
                    "Previous saved data in path: " + offsetsFile.getPath() + " didn't found."
                    + "It's ok if this storage launches first time or didn't save data before"
            );
            this.storageOffsets = MemorySegment.NULL;
            return;
        }

        try (RandomAccessFile file = new RandomAccessFile(offsetsFile, "r");
             FileChannel channel = file.getChannel()) {

            this.storageOffsets = channel.map(FileChannel.MapMode.READ_ONLY, 0, channel.size(), Arena.ofAuto());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private E findInStorage(D key) {
        // null means that wasn't read
        if (storageOffsets == null) {
            readOffsets();
        }
        // MemorySegment.NULL means that file doesn't exist (ex. first launch, no need trying to read again
        if (storageOffsets == MemorySegment.NULL) {
            return null;
        }

        File databaseFile = basePath.resolve(DB_FILENAME).toFile();
        if (!databaseFile.exists()) {
            logger.severe(() ->
                    "Previous saved data in path: " + databaseFile.getPath()
                    + " didn't found, + but offsets file exist."
            );
            return null;
        }

        try (RandomAccessFile file = new RandomAccessFile(databaseFile, "r");
             FileChannel channel = file.getChannel()) {
            Arena arena = Arena.ofAuto();
            MemorySegment fileSegment = channel.map(FileChannel.MapMode.READ_ONLY, 0, channel.size(), arena);
            
            // binary search
            D foundedKey = null;
            long leftOffsetIndex = -1; // offset of element <= key
            long rightOffsetIndex = storageOffsets.byteSize() / Long.BYTES; // offset of element > key

            while (leftOffsetIndex + 1 < rightOffsetIndex) {
                long middleOffsetIndex = leftOffsetIndex + (rightOffsetIndex - leftOffsetIndex) / 2;
                long middleOffset = storageOffsets.getAtIndex(LONG_LAYOUT, middleOffsetIndex);

                D currentKey = readValue(fileSegment, middleOffset);
                int compared = comparator.compare(currentKey, key);

                if (compared < 0) {
                    leftOffsetIndex = middleOffsetIndex;
                } else if (compared > 0) {
                    rightOffsetIndex = middleOffsetIndex;
                } else {
                    leftOffsetIndex = middleOffsetIndex;
                    foundedKey = currentKey;
                    break;
                }
            }
            if (foundedKey == null) {
                // not found, element at leftOffset < key
                return null;
            }

            long valueOffset = storageOffsets.getAtIndex(LONG_LAYOUT, leftOffsetIndex);
            valueOffset += SIZE_LENGTH;
            valueOffset += serializer.size(foundedKey);
            D value = readValue(fileSegment, valueOffset);

            return serializer.createEntry(foundedKey, value);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    //  ===================================
    //  Writing offsets and data
    //  ===================================

    private void writeData() throws IOException {
        // Merge data from disk with memory DAO and write updated file
        addDataFromStorage();

        Path dbPath = basePath.resolve(DB_FILENAME);
        Path offsetsPath = basePath.resolve(OFFSETS_FILENAME);

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

    private void addDataFromStorage() throws IOException {
        Path dbPath = basePath.resolve(DB_FILENAME);
        if (!Files.exists(dbPath)) {
            return;
        }

        try (FileChannel db = FileChannel.open(dbPath, StandardOpenOption.READ);
             Arena arena = Arena.ofConfined()) {

            long dbSize = db.size();
            MemorySegment dbMemorySegment = db.map(FileChannel.MapMode.READ_ONLY, 0, dbSize, arena);

            long offset = 0;
            while (offset < dbSize) {
                D key = readValue(dbMemorySegment, offset);
                offset += SIZE_LENGTH;
                offset += serializer.size(key);

                D value = readValue(dbMemorySegment, offset);
                offset += SIZE_LENGTH;
                offset += serializer.size(value);

                E entry = serializer.createEntry(key, value);
                dao.putIfAbsent(key, entry);
            }
        }
    }

    //  ===================================
    //  Some util methods
    //  ===================================

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
