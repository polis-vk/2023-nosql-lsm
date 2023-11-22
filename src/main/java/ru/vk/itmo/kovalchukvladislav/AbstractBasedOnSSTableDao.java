package ru.vk.itmo.kovalchukvladislav;

import ru.vk.itmo.Config;
import ru.vk.itmo.Entry;
import ru.vk.itmo.kovalchukvladislav.model.DaoIterator;
import ru.vk.itmo.kovalchukvladislav.model.EntryExtractor;
import ru.vk.itmo.kovalchukvladislav.model.TableInfo;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.logging.Level;
import java.util.logging.Logger;

public abstract class AbstractBasedOnSSTableDao<D, E extends Entry<D>> extends AbstractInMemoryDao<D, E> {
    //  ===================================
    //  Constants
    //  ===================================
    private static final String METADATA_FILENAME = "metadata";
    private static final String OFFSETS_FILENAME_PREFIX = "offsets_";
    private static final String DB_FILENAME_PREFIX = "db_";

    //  ===================================
    //  Variables
    //  ===================================

    private final Path basePath;
    private final Arena arena = Arena.ofShared();
    private final EntryExtractor<D, E> extractor;
    private final SSTableMemorySegmentWriter<D, E> writer;

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
        this.dbMappedSegments = new ArrayList<>();
        this.offsetMappedSegments = new ArrayList<>();
        reloadFilesAndMapToSegment();
        this.writer = new SSTableMemorySegmentWriter<>(basePath, DB_FILENAME_PREFIX, OFFSETS_FILENAME_PREFIX,
                METADATA_FILENAME, extractor);
        logger.setLevel(Level.OFF); // чтобы не засорять вывод в гитхабе, если такое возможно
    }

    //  ===================================
    //  Restoring state
    //  ===================================

    private void reloadFilesAndMapToSegment() throws IOException {
        if (!Files.exists(basePath)) {
            Files.createDirectory(basePath);
        }
        logger.info(() -> String.format("Reloading files from %s", basePath));
        List<String> ssTableIds = getSSTableIds();
        for (String ssTableId : ssTableIds) {
            readFileAndMapToSegment(ssTableId);
        }
        logger.info(() -> String.format("Reloaded %d files", storagesCount));
    }

    private void readFileAndMapToSegment(String timestamp) throws IOException {
        Path dbPath = basePath.resolve(DB_FILENAME_PREFIX + timestamp);
        Path offsetsPath = basePath.resolve(OFFSETS_FILENAME_PREFIX + timestamp);
        if (!Files.exists(dbPath) || !Files.exists(offsetsPath)) {
            logger.severe(() -> String.format("File under path %s or %s doesn't exists", dbPath, offsetsPath));
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

    private List<String> getSSTableIds() throws IOException {
        Path metadataPath = basePath.resolve(METADATA_FILENAME);
        if (!Files.exists(metadataPath)) {
            return Collections.emptyList();
        }
        return Files.readAllLines(metadataPath, StandardCharsets.UTF_8);
    }

    private Path[] getAllTablesPath() throws IOException {
        List<String> ssTableIds = getSSTableIds();
        int size = ssTableIds.size();
        Path[] files = new Path[2 * size];

        for (int i = 0; i < size; i++) {
            String id = ssTableIds.get(i);
            files[2 * i] = basePath.resolve(DB_FILENAME_PREFIX + id);
            files[2 * i + 1] = basePath.resolve(OFFSETS_FILENAME_PREFIX + id);
        }
        return files;
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
    //  Some utils
    //  ===================================

    private TableInfo getInMemoryDaoSizeInfo() {
        long size = 0;
        for (E entry : dao.values()) {
            size += extractor.size(entry);
        }
        return new TableInfo(dao.size(), size);
    }

    private TableInfo getSSTableDaoSizeInfo() {
        Iterator<E> allIterator = all();
        long entriesCount = 0;
        long daoSize = 0;

        while (allIterator.hasNext()) {
            E next = allIterator.next();
            entriesCount++;
            daoSize += extractor.size(next);
        }

        return new TableInfo(entriesCount, daoSize);
    }

    //  ===================================
    //  Flush and close
    //  ===================================

    @Override
    public synchronized void flush() throws IOException {
        if (dao.isEmpty()) {
            return;
        }
        writer.flush(dao.values().iterator(), getInMemoryDaoSizeInfo());
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
        if (storagesCount <= 1 && dao.isEmpty()) {
            return;
        }
        Path[] oldTables = getAllTablesPath();
        writer.compact(all(), getSSTableDaoSizeInfo());
        writer.deleteUnusedFiles(oldTables);
    }
}
