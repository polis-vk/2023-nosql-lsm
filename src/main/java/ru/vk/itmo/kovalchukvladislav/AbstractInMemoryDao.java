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
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Comparator;
import java.util.Iterator;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

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
 * <p>This allows search entries using a binary search after <tt>db</tt> reading (usually less due to missing values)
 */
public abstract class AbstractInMemoryDao<D, E extends Entry<D>> implements Dao<D, E> {
    private static final String DB_FILENAME = "db";
    private static final int SIZE_LENGTH = Long.BYTES;
    private static final String OFFSETS_FILENAME = "offsets";

    private final Path basePath;
    private volatile boolean closed = false;
    private volatile long[] storageOffsets = null;
    private final Comparator<? super D> comparator;
    private final ConcurrentNavigableMap<D, E> dao;
    private final MemorySegmentSerializer<D, E> serializer;

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
        long[] offsets = writeEntries();
        writeOffsets(offsets);
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
        long size = memorySegment.get(ValueLayout.JAVA_LONG_UNALIGNED, offset);
        MemorySegment valueSegment = memorySegment.asSlice(offset + SIZE_LENGTH, size);
        return serializer.toValue(valueSegment);
    }

    // Return new offset
    private long writeValue(D value, MemorySegment memorySegment, long offset) {
        MemorySegment valueSegment = serializer.fromValue(value);
        long size = valueSegment.byteSize();

        memorySegment.set(ValueLayout.JAVA_LONG_UNALIGNED, offset, size);
        MemorySegment.copy(valueSegment, 0, memorySegment, offset + SIZE_LENGTH, size);
        return offset + SIZE_LENGTH + size;
    }

    //  ===================================
    //  Reading offsets and data
    //  ===================================

    /**
     * Read offsets from file.<br>
     * If file doesn't exist will be filled with empty array and output a warning message.<br>
     * After completion of work storageOffsets not null.
     */
    private synchronized void readOffsets() {
        if (storageOffsets != null) {
            return;
        }
        File offsetsFile = basePath.resolve(OFFSETS_FILENAME).toFile();
        if (!offsetsFile.exists()) {
            // Предположим что тут и далее нормальный логгер, и мы вызываем log.warning(), log.error() вместо этого
            System.out.println(
                    "[WARN] Previous saved data in path: " + offsetsFile.getPath() + " didn't found."
                            + "It's ok if this storage launches first time or didn't save data before"
            );
            this.storageOffsets = new long[] {};
            return;
        }

        try (RandomAccessFile file = new RandomAccessFile(offsetsFile, "r");
             FileChannel channel = file.getChannel();
             Arena arena = Arena.ofConfined()) {

            MemorySegment fileSegment = channel.map(FileChannel.MapMode.READ_ONLY, 0, channel.size(), arena);
            this.storageOffsets = fileSegment.toArray(ValueLayout.OfLong.JAVA_LONG);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private E findInStorage(D key) {
        if (storageOffsets == null) {
            readOffsets();
        }
        if (storageOffsets.length == 0) {
            return null;
        }

        File databaseFile = basePath.resolve(DB_FILENAME).toFile();
        if (!databaseFile.exists()) {
            System.out.println(
                    "[ERROR] Previous saved data in path: " + databaseFile.getPath() + " didn't found, "
                            + "but offsets file exist."
            );
            return null;
        }

        try (RandomAccessFile file = new RandomAccessFile(databaseFile, "r");
             FileChannel channel = file.getChannel()) {
            Arena arena = Arena.ofAuto();
            MemorySegment fileSegment = channel.map(FileChannel.MapMode.READ_ONLY, 0, channel.size(), arena);

            // binary search
            D foundedKey = null;
            int leftOffsetIndex = -1; // offset of element <= key
            int rightOffsetIndex = storageOffsets.length; // offset of element > key

            while (leftOffsetIndex + 1 < rightOffsetIndex) {
                int middleOffsetIndex = leftOffsetIndex + (rightOffsetIndex - leftOffsetIndex) / 2;
                long middleOffset = storageOffsets[middleOffsetIndex];

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

            long valueOffset = storageOffsets[leftOffsetIndex] + SIZE_LENGTH + serializer.size(foundedKey);
            D value = readValue(fileSegment, valueOffset);

            return serializer.createEntry(foundedKey, value);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    //  ===================================
    //  Writing offsets and data
    //  ===================================

    // Return nullable offsets

    private long[] writeEntries() throws IOException {
        Path resultPath = basePath.resolve(DB_FILENAME);

        try (FileChannel channel = FileChannel.open(
                resultPath,
                StandardOpenOption.READ,
                StandardOpenOption.WRITE,
                StandardOpenOption.TRUNCATE_EXISTING,
                StandardOpenOption.CREATE);
             Arena arena = Arena.ofConfined()) {

            long size = calculateInMemoryDAOSize();
            MemorySegment fileSegment = channel.map(FileChannel.MapMode.READ_WRITE, 0, size, arena);

            int i = 0;
            long offset = 0;
            long[] offsets = new long[dao.size()];
            for (E entry : dao.values()) {
                offsets[i++] = offset;
                offset = writeValue(entry.key(), fileSegment, offset);
                offset = writeValue(entry.value(), fileSegment, offset);
            }
            fileSegment.load();
            return offsets;
        }
    }

    private void writeOffsets(long[] offsets) throws IOException {
        Path resultPath = basePath.resolve(OFFSETS_FILENAME);

        try (FileChannel channel = FileChannel.open(
                resultPath,
                StandardOpenOption.READ,
                StandardOpenOption.WRITE,
                StandardOpenOption.TRUNCATE_EXISTING,
                StandardOpenOption.CREATE);
             Arena arena = Arena.ofConfined()) {

            long size = (long) offsets.length * SIZE_LENGTH;
            MemorySegment fileSegment = channel.map(FileChannel.MapMode.READ_WRITE, 0, size, arena);
            MemorySegment offsetsSegment = MemorySegment.ofArray(offsets);
            MemorySegment.copy(offsetsSegment, 0, fileSegment, 0, size);
            fileSegment.load();
        }
    }

    //  ===================================
    //  Some util methods
    //  ===================================

    private long calculateInMemoryDAOSize() {
        long size = 0;
        for (E entry : dao.values()) {
            size += 2 * SIZE_LENGTH;
            size += serializer.size(entry.key());
            size += serializer.size(entry.value());
        }
        return size;
    }
}
