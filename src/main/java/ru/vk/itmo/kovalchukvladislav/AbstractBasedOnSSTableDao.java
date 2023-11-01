package ru.vk.itmo.kovalchukvladislav;

import ru.vk.itmo.Config;
import ru.vk.itmo.Entry;
import ru.vk.itmo.kovalchukvladislav.model.DaoIterator;
import ru.vk.itmo.kovalchukvladislav.model.EntryExtractor;

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
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

public abstract class AbstractBasedOnSSTableDao<D, E extends Entry<D>> extends AbstractInMemoryDao<D, E> {
    //  ===================================
    //  Constants
    //  ===================================
    private static final ValueLayout.OfLong LONG_LAYOUT = ValueLayout.JAVA_LONG_UNALIGNED;
    private static final String OFFSETS_FILENAME_PREFIX = "offsets_";
    private static final String METADATA_FILENAME = "metadata";
    private static final String DB_FILENAME_PREFIX = "db_";

    //  ===================================
    //  Variables
    //  ===================================

    private final Path basePath;
    private final Path metadataPath;
    private final Arena arena = Arena.ofShared();
    private final EntryExtractor<D, E> extractor;

    //  ===================================
    //  Storages
    //  ===================================

    private final int storagesCount;
    private final List<MemorySegment> dbMappedSegments;
    private final List<MemorySegment> offsetMappedSegments;

    protected AbstractBasedOnSSTableDao(Config config, EntryExtractor<D, E> extractor) throws IOException {
        super(extractor);
        this.extractor = extractor;
        this.basePath = Objects.requireNonNull(config.basePath());

        if (!Files.exists(basePath)) {
            Files.createDirectory(basePath);
        }
        this.metadataPath = basePath.resolve(METADATA_FILENAME);

        this.storagesCount = getCountFromMetadataOrCreate();
        this.dbMappedSegments = new ArrayList<>(storagesCount);
        this.offsetMappedSegments = new ArrayList<>(storagesCount);

        for (int i = 0; i < storagesCount; i++) {
            readFileAndMapToSegment(DB_FILENAME_PREFIX, i, dbMappedSegments);
            readFileAndMapToSegment(OFFSETS_FILENAME_PREFIX, i, offsetMappedSegments);
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
                                         List<MemorySegment> segments) throws IOException {
        Path path = basePath.resolve(filenamePrefix + index);
        try (FileChannel channel = FileChannel.open(path, StandardOpenOption.READ)) {

            MemorySegment segment = channel.map(FileChannel.MapMode.READ_ONLY, 0, Files.size(path), arena);
            segments.add(segment);
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
             Arena confinedArena = Arena.ofConfined()) {

            long dbSize = getDAOBytesSize();
            long offsetsSize = (long) dao.size() * Long.BYTES;
            MemorySegment fileSegment = db.map(FileChannel.MapMode.READ_WRITE, 0, dbSize, confinedArena);
            MemorySegment offsetsSegment = offsets.map(FileChannel.MapMode.READ_WRITE, 0, offsetsSize, confinedArena);

            int i = 0;
            long offset = 0;
            for (E entry : dao.values()) {
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
    }
}
