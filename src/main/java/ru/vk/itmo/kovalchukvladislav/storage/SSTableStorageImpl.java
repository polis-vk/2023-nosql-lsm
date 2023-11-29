package ru.vk.itmo.kovalchukvladislav.storage;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.logging.Logger;
import ru.vk.itmo.Entry;
import ru.vk.itmo.kovalchukvladislav.model.SimpleDaoLoggerFactory;

public class SSTableStorage<D, E extends Entry<D>> {
    private static final Logger logger = SimpleDaoLoggerFactory.createLogger(SSTableStorage.class);
    private final Path basePath;
    private final String metadataFilename;
    private final String dbFilenamePrefix;
    private final String offsetsFilenamePrefix;
    private final Arena arena = Arena.ofShared();
    private final ReadWriteLock storageChangeLock = new ReentrantReadWriteLock();
    private final ExecutorService backgroundQueue = Executors.newSingleThreadExecutor();

    // Следущие три поля меняются одновременно и атомарно с storageChangeLock
    private final AtomicLong storagesCount = new AtomicLong(0);
    private final List<MemorySegment> dbMappedSegments;
    private final List<MemorySegment> offsetMappedSegments;

    public SSTableStorage(Path basePath,
                          String metadataFilename,
                          String dbFilenamePrefix,
                          String offsetsFilenamePrefix) throws IOException {
        this.basePath = basePath;
        this.metadataFilename = metadataFilename;
        this.dbFilenamePrefix = dbFilenamePrefix;
        this.offsetsFilenamePrefix = offsetsFilenamePrefix;
        if (!Files.exists(basePath)) {
            Files.createDirectory(basePath);
        }
    }

    private void reloadFilesAndMapToSegment() throws IOException {
        logger.info(() -> String.format("Reloading files from %s", basePath));
        List<String> ssTableIds = getSSTableIds();
        List<MemorySegment>
        for (String ssTableId : ssTableIds) {
            readFileAndMapToSegment(ssTableId);
        }
        logger.info(() -> String.format("Reloaded %d files", storagesCount));
    }

    private void readFileAndMapToSegment(List<MemorySegment> dbMappedResult,
                                         List<MemorySegment> offsetMappedResult,
                                         String timestamp) throws IOException {
        Path dbPath = basePath.resolve(dbFilenamePrefix + timestamp);
        Path offsetsPath = basePath.resolve(offsetsFilenamePrefix + timestamp);
        if (!Files.exists(dbPath) || !Files.exists(offsetsPath)) {
            throw new FileNotFoundException("File under path " + dbPath + " or " + offsetsPath + " doesn't exists");
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
        storageChangeLock.readLock().lock();
        try {
            Path metadataPath = basePath.resolve(metadataFilename);
            if (!Files.exists(metadataPath)) {
                return Collections.emptyList();
            }
            return Files.readAllLines(metadataPath, StandardCharsets.UTF_8);
        } finally {
            storageChangeLock.readLock().unlock();
        }
    }

    private Path[] getAllTablesPath() throws IOException {
        List<String> ssTableIds = getSSTableIds();
        int size = ssTableIds.size();
        Path[] files = new Path[2 * size];

        for (int i = 0; i < size; i++) {
            String id = ssTableIds.get(i);
            files[2 * i] = basePath.resolve(dbFilenamePrefix + id);
            files[2 * i + 1] = basePath.resolve(offsetsFilenamePrefix + id);
        }
        return files;
    }
}
