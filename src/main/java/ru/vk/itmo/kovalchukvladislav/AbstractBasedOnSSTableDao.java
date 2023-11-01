package ru.vk.itmo.kovalchukvladislav;

import ru.vk.itmo.Config;
import ru.vk.itmo.Entry;
import ru.vk.itmo.kovalchukvladislav.model.DaoIterator;
import ru.vk.itmo.kovalchukvladislav.model.EntryExtractor;

import java.io.File;
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
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.logging.Logger;

public abstract class AbstractBasedOnSSTableDao<D, E extends Entry<D>> extends AbstractInMemoryDao<D, E> {
    //  ===================================
    //  Constants
    //  ===================================
    private static final ValueLayout.OfLong LONG_LAYOUT = ValueLayout.JAVA_LONG_UNALIGNED;
    private static final String OFFSETS_FILENAME_PREFIX = "offsets_";
    private static final String DB_FILENAME_PREFIX = "db_";

    //  ===================================
    //  Variables
    //  ===================================

    private final Path basePath;
    private final Arena arena = Arena.ofShared();
    private final EntryExtractor<D, E> extractor;

    //  ===================================
    //  Storages
    //  ===================================

    private int storagesCount;
    private volatile boolean closed;
    private final List<MemorySegment> dbMappedSegments;
    private final List<MemorySegment> offsetMappedSegments;
    private final Logger logger = Logger.getLogger(getClass().getSimpleName());

    protected AbstractBasedOnSSTableDao(Config config, EntryExtractor<D, E> extractor) throws IOException {
        super(extractor);
        this.closed = false;
        this.storagesCount = 0;
        this.extractor = extractor;
        this.basePath = Objects.requireNonNull(config.basePath());

        if (!Files.exists(basePath)) {
            Files.createDirectory(basePath);
        }
        this.dbMappedSegments = new ArrayList<>();
        this.offsetMappedSegments = new ArrayList<>();
        reloadFilesAndMapToSegment();
    }

    //  ===================================
    //  Restoring state
    //  ===================================

    private void reloadFilesAndMapToSegment() throws IOException {
        logger.info(() -> "Reloading files");
        File dir = new File(basePath.toString());
        File[] files = dir.listFiles(it -> it.getName().startsWith(DB_FILENAME_PREFIX));
        if (files == null) {
            return;
        }
        Arrays.sort(files);

        for (File file : files) {
            String name = file.getName();
            int index = name.indexOf(DB_FILENAME_PREFIX);
            if (index == -1) {
                continue;
            }
            String timestamp = name.substring(index + DB_FILENAME_PREFIX.length());
            readFileAndMapToSegment(timestamp);
        }
        logger.info(() -> String.format("Reloaded %d files", storagesCount));
    }

    private void readFileAndMapToSegment(String timestamp) throws IOException {
        Path dbPath = basePath.resolve(DB_FILENAME_PREFIX + timestamp);
        Path offsetsPath = basePath.resolve(OFFSETS_FILENAME_PREFIX + timestamp);
        if (!Files.exists(dbPath) || !Files.exists(offsetsPath)) {
            return;
        }

        logger.info(() -> String.format("Reading files with timestamp %s", timestamp));

        try (FileChannel dbChannel = FileChannel.open(dbPath, StandardOpenOption.READ);
             FileChannel offsetChannel = FileChannel.open(offsetsPath, StandardOpenOption.READ)) {

            MemorySegment db = dbChannel.map(FileChannel.MapMode.READ_ONLY, 0, Files.size(dbPath), arena);
            MemorySegment offsets = offsetChannel.map(FileChannel.MapMode.READ_ONLY, 0, Files.size(offsetsPath), arena);
            dbMappedSegments.add(db);
            offsetMappedSegments.add(offsets);
            storagesCount++;
        }

        logger.info(() -> String.format("Successfully read files with %s timestamp", timestamp));
    }


    //  ===================================
    //  Finding in storage
    //  ===================================
    @Override
    public Iterator<E> get(D from, D to) {
        Iterator<E> inMemotyIterator = super.get(from, to);
        return new DaoIterator<>(from, to, inMemotyIterator, dbMappedSegments, offsetMappedSegments, extractor);
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

            long offset = extractor.findLowerBoundValueOffset(key, storage, offsets);
            if (offset == -1) {
                continue;
            }
            D lowerBoundKey = extractor.readValue(storage, offset);

            if (comparator.compare(lowerBoundKey, key) == 0) {
                long valueOffset = offset + extractor.size(lowerBoundKey);
                D value = extractor.readValue(storage, valueOffset);
                return extractor.createEntry(lowerBoundKey, value);
            }
        }
        return null;
    }

    //  ===================================
    //  Writing data
    //  ===================================

    private long writeMemoryDAO() throws IOException {
        return writeData(dao.values().iterator(), dao.size(), getDAOBytesSize());
    }

    private long writeMemoryAndStorageDAO() throws IOException {
        Iterator<E> allIterator = all();
        long entriesCount = 0;
        long daoSize = 0;

        while (allIterator.hasNext()) {
            E next = allIterator.next();
            entriesCount++;
            daoSize += extractor.size(next);
        }
        return writeData(all(), entriesCount, daoSize);
    }

    /**
     * Returns created files timestamp.
     */
    private long writeData(Iterator<E> daoIterator, long entriesCount, long daoSize) throws IOException {
        long timestamp = System.currentTimeMillis();
        Path dbPath = basePath.resolve(DB_FILENAME_PREFIX + timestamp);
        Path offsetsPath = basePath.resolve(OFFSETS_FILENAME_PREFIX + timestamp);

        logger.info(() -> String.format("Writing files with %s timestamp", timestamp));

        OpenOption[] options = new OpenOption[] {
                StandardOpenOption.READ,
                StandardOpenOption.WRITE,
                StandardOpenOption.TRUNCATE_EXISTING,
                StandardOpenOption.CREATE
        };

        try (FileChannel db = FileChannel.open(dbPath, options);
             FileChannel offsets = FileChannel.open(offsetsPath, options);
             Arena confinedArena = Arena.ofConfined()) {

            long offsetsSize = entriesCount * Long.BYTES;
            MemorySegment fileSegment = db.map(FileChannel.MapMode.READ_WRITE, 0, daoSize, confinedArena);
            MemorySegment offsetsSegment = offsets.map(FileChannel.MapMode.READ_WRITE, 0, offsetsSize, confinedArena);

            int i = 0;
            long offset = 0;
            while (daoIterator.hasNext()) {
                E entry = daoIterator.next();
                offsetsSegment.setAtIndex(LONG_LAYOUT, i, offset);
                i += 1;
                offset = extractor.writeEntry(entry, fileSegment, offset);
            }
            fileSegment.load();
            offsetsSegment.load();
        }

        logger.info(() -> String.format("Successfully writing with %s timestamp. Entries count %d, daoSize %d",
                timestamp, entriesCount, daoSize));

        return timestamp;
    }

    private long getDAOBytesSize() {
        long size = 0;
        for (E entry : dao.values()) {
            size += extractor.size(entry);
        }
        return size;
    }

    //  ===================================
    //  Flush and close
    //  ===================================

    @Override
    public synchronized void flush() throws IOException {
        if (!dao.isEmpty()) {
            long flushedFileName = writeMemoryDAO();
            logger.info(() -> String.format("Flushed timestamp is %d", flushedFileName));
        }
    }

    @Override
    public synchronized void close() throws IOException {
        if (closed) {
            return;
        }
        flush();
        if (arena.scope().isAlive()) {
            arena.close();
        }
        closed = true;
    }

    @Override
    public synchronized void compact() throws IOException {
        long createdFileTimestamp = writeMemoryAndStorageDAO();
        logger.info(() -> String.format("Compacted timestamp is %d", createdFileTimestamp));
        deleteFilesExceptWithTimeStamp(String.valueOf(createdFileTimestamp));
    }

    private void deleteFilesExceptWithTimeStamp(String excludedTimeStamp) throws IOException {
        File dir = new File(basePath.toString());

        File[] files = dir.listFiles(it -> shouldDelete(it.getName(), excludedTimeStamp));
        if (files == null) {
            logger.info(() -> String.format("Not found files to delete, excludedTimeStamp %s", excludedTimeStamp));
            return;
        }
        Arrays.sort(files);
        for (File file : files) {
            Files.delete(file.toPath());
            logger.info(() -> String.format("Delete %s file", file.getName()));
        }
    }

    private boolean shouldDelete(String fileName, String excludedTimeStamp) {
        return (fileName.startsWith(DB_FILENAME_PREFIX) || fileName.startsWith(OFFSETS_FILENAME_PREFIX))
                && !fileName.endsWith(excludedTimeStamp);
    }
}
