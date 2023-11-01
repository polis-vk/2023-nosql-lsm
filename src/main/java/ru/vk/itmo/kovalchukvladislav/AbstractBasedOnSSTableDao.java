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
import java.util.concurrent.atomic.AtomicInteger;

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

    private final AtomicInteger storagesCount = new AtomicInteger(0);
    private final List<MemorySegment> dbMappedSegments;
    private final List<MemorySegment> offsetMappedSegments;

    protected AbstractBasedOnSSTableDao(Config config, EntryExtractor<D, E> extractor) throws IOException {
        super(extractor);
        this.extractor = extractor;
        this.basePath = Objects.requireNonNull(config.basePath());

        if (!Files.exists(basePath)) {
            Files.createDirectory(basePath);
        }
        this.dbMappedSegments = new ArrayList<>();
        this.offsetMappedSegments = new ArrayList<>();
        readFilesAndMapToSegment();
    }

    //  ===================================
    //  Restoring state
    //  ===================================

    private void readFilesAndMapToSegment() throws IOException {
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
    }

    private void readFileAndMapToSegment(String timestamp) throws IOException {
        Path dbPath = basePath.resolve(DB_FILENAME_PREFIX + timestamp);
        Path offsetsPath = basePath.resolve(OFFSETS_FILENAME_PREFIX + timestamp);
        if (!Files.exists(dbPath) || !Files.exists(offsetsPath)) {
            return;
        }
        try (FileChannel dbChannel = FileChannel.open(dbPath, StandardOpenOption.READ);
             FileChannel offsetChannel = FileChannel.open(offsetsPath, StandardOpenOption.READ)) {

            MemorySegment db = dbChannel.map(FileChannel.MapMode.READ_ONLY, 0, Files.size(dbPath), arena);
            MemorySegment offsets = offsetChannel.map(FileChannel.MapMode.READ_ONLY, 0, Files.size(offsetsPath), arena);
            dbMappedSegments.add(db);
            offsetMappedSegments.add(offsets);
            storagesCount.incrementAndGet();
        }
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
        for (int i = storagesCount.get() - 1; i >= 0; i--) {
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

    private void writeMemoryDAO() throws IOException {
        writeData(dao.values().iterator(), dao.size(), getDAOBytesSize());
    }

    private void writeMemoryAndStorageDAO() throws IOException {
        Iterator<E> allIterator = all();
        long entriesCount = 0;
        long daoSize = 0;

        while (allIterator.hasNext()) {
            E next = allIterator.next();
            entriesCount++;
            daoSize += extractor.size(next);
        }
        writeData(all(), entriesCount, daoSize);
    }

    private void writeData(Iterator<E> daoIterator, long entriesCount, long daoSize) throws IOException {
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
    private void clear() {
        dao.clear();
        dbMappedSegments.clear();
        offsetMappedSegments.clear();
        storagesCount.set(0);
    }

    @Override
    public synchronized void flush() throws IOException {
        if (!dao.isEmpty()) {
            writeMemoryDAO();
        }
    }

    @Override
    public synchronized void close() throws IOException {
        compact();
        if (arena.scope().isAlive()) {
            arena.close();
        }
    }

    @Override
    public void compact() throws IOException {
        writeMemoryAndStorageDAO();
        clear();
        deleteFilesExceptLatest(DB_FILENAME_PREFIX);
        deleteFilesExceptLatest(OFFSETS_FILENAME_PREFIX);
        readFilesAndMapToSegment();
    }

    private void deleteFilesExceptLatest(String prefix) throws IOException {
        File dir = new File(basePath.toString());
        File[] files = dir.listFiles(it -> it.getName().startsWith(prefix));
        if (files == null) {
            return;
        }
        Arrays.sort(files);
        for (int i = 0; i + 1 < files.length; i++) {
            Files.delete(files[i].toPath());
        }
    }
}
