package ru.vk.itmo.smirnovdmitrii.outofmemory.sstable;

import ru.vk.itmo.smirnovdmitrii.outofmemory.IndexFileRecord;
import ru.vk.itmo.smirnovdmitrii.util.exceptions.CorruptedException;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.nio.channels.FileChannel;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Storage of SSTables. Manages SSTables.
 */
public class SSTableStorageImpl implements SSTableStorage {
    // Sorted by SSTable priority.
    private final AtomicReference<List<SSTable>> storage = new AtomicReference<>(null);
    private final ExecutorService deleter = Executors.newSingleThreadExecutor();
    private final Path basePath;
    private final String indexFileName;
    private AtomicLong priorityCounter;
    private final Lock indexFileLock = new ReentrantLock();
    private final Arena arena = Arena.ofShared();

    public SSTableStorageImpl(final Path basePath, final String indexFileName) throws IOException {
        this.basePath = basePath;
        this.indexFileName = indexFileName;
        load();
    }

    /**
     * Loading SSTables from index file.
     * @throws IOException if I/O error occurs.
     */
    private void load() throws IOException {
        final List<String> lines = Files.readAllLines(basePath.resolve(indexFileName)).reversed();
        final Set<Long> compacted = new HashSet<>();
        final Set<String> ssTableNames = new HashSet<>();
        long maxPriority = 0;
        final List<SSTable> ssTables = new ArrayList<>();
        for (final String indexRecord : lines) {
            final IndexFileRecord indexFileRecord = new IndexFileRecord(indexRecord);
            final long priority = indexFileRecord.getPriority();
            maxPriority = Math.max(maxPriority, priority);
            if (!compacted.remove(priority) && !indexFileRecord.getName().equals("delete")) {
                final Path tablePath = basePath.resolve(indexFileRecord.getName());
                if (Files.notExists(tablePath)) {
                    throw new CorruptedException("corrupted: file from record [" + indexRecord + "] not found.");
                }
                try (FileChannel channel = FileChannel.open(tablePath, StandardOpenOption.READ)) {
                    ssTables.add(
                            new SSTable(
                                    channel.map(FileChannel.MapMode.READ_ONLY, 0, channel.size(), arena),
                                    tablePath,
                                    priority
                            )
                    );
                }
                ssTableNames.add(indexFileRecord.getName());

            }
            if (indexFileRecord.isCompaction()) {
                compacted.addAll(indexFileRecord.getCompactedPriorities());
            }
        }
        storage.set(ssTables);
        priorityCounter = new AtomicLong(maxPriority + 1);
        Files.walkFileTree(basePath, new SimpleFileVisitor<>() {
            @Override
            public FileVisitResult visitFile(final Path file, final BasicFileAttributes attrs) throws IOException {
                if (!ssTableNames.contains(file.getFileName().toString())) {
                    Files.delete(file);
                }
                return FileVisitResult.CONTINUE;
            }
        });
        final List<String> newIndexFileLines = new ArrayList<>();
        for (int i = ssTables.size() - 1; i >= 0; i--) {
            newIndexFileLines.add(ssTableIndexRecord(ssTables.get(i)));
        }
        newIndex(newIndexFileLines);
    }

    /**
     * Adding SSTable to storage from given file name.
     * @param ssTableFileName SSTable to add.
     * @throws IOException if I/O error occurs.
     */
    @Override
    public void add(final String ssTableFileName) throws IOException {
        if (ssTableFileName == null) {
            return;
        }
        final Path ssTablePath = basePath.resolve(ssTableFileName);
        final MemorySegment mappedSSTable = map(ssTablePath);
        final SSTable ssTable = new SSTable(mappedSSTable, ssTablePath, priorityCounter.getAndIncrement());
        appendToIndex(ssTableIndexRecord(ssTable));
        while (true) {
            final List<SSTable> oldSSTables = storage.get();
            final List<SSTable> newSSTables = new ArrayList<>();
            newSSTables.add(ssTable);
            newSSTables.addAll(oldSSTables);
            if (storage.compareAndSet(oldSSTables, newSSTables)) {
                break;
            }
        }
    }

    /**
     * Returns where SSTable was compacted. If given SSTable is alive, then unspecified.
     * @param ssTable SSTable that was compacted (dead)
     * @return SSTable where provided SSTable was compacted.
     */
    @Override
    public SSTable getCompaction(final SSTable ssTable) {
        // While we're compacting all SSTable in one, always returns last SSTable.
        final List<SSTable> currentStorage = storage.get();
        if (currentStorage.isEmpty()) {
            return null;
        }
        return currentStorage.getLast();
    }

    /**
     * Replaces Provided SSTables with SSTable from provided path.
     * @param compactionFileName file with compacted data from SSTables.
     * @param compacted representing files that was compacted.
     * @throws IOException if I/O error occurs.
     */
    @Override
    public void compact(final String compactionFileName, final List<SSTable> compacted) throws IOException {
        SSTable compaction = null;
        if (compactionFileName != null) {
            final Path compactionPath = basePath.resolve(compactionFileName);
            final MemorySegment mappedCompaction = map(compactionPath);
            long minPriority = Long.MAX_VALUE;
            // Taking minimal priority for reason if we want to compact in the mid in future.
            for (final SSTable ssTable : compacted) {
                minPriority = Math.min(ssTable.priority(), minPriority);
            }
            compaction = new SSTable(mappedCompaction, compactionPath, minPriority);
        }
        appendToIndex(compactionIndexRecord(compaction, compacted));
        final SSTable firstCompacted = compacted.getFirst();
        // tries to add SSTable to list with CAS. If failed - tries again.
        while (true) {
            final List<SSTable> oldSSTables = storage.get();
            final List<SSTable> newSSTables = new ArrayList<>();
            int index = 0;
            // Adding all SSTable that was not compacted. They have greater priority.
            while (oldSSTables.get(index).priority() != firstCompacted.priority()) {
                newSSTables.add(oldSSTables.get(index));
                index++;
            }
            if (compaction != null) {
                newSSTables.add(compaction);
            }
            if (storage.compareAndSet(oldSSTables, newSSTables)) {
                break;
            }
        }
        for (final SSTable ssTable: compacted) {
            ssTable.kill();
        }
        deleter.execute(() -> deleteTask(compacted));
    }

    private MemorySegment map(final Path path) throws IOException {
        try (FileChannel channel = FileChannel.open(path, StandardOpenOption.READ)) {
            return channel.map(FileChannel.MapMode.READ_ONLY, 0, channel.size(), arena);
        }
    }

    private void deleteTask(final List<SSTable> compacted) {
        int deleted = 0;
        while (deleted < compacted.size()) {
            for (int i = 0; i < compacted.size(); i++) {
                final SSTable ssTable = compacted.get(i);
                // If still has readers then we can't delete file.
                if (ssTable == null || ssTable.readers().get() != 0) {
                    continue;
                }
                try {
                    Files.delete(ssTable.path());
                } catch (final IOException e) {
                    throw new UncheckedIOException(e);
                }
                compacted.set(i, null);
                deleted++;
            }
        }
    }

    /**
     * Record for SSTable in index file.
     * Looks like:<br>
     * <br>
     * sstable sstable_file_name priority<br>
     * <br>
     * <br>
     * Example:
     * <br>
     * sstable sstable_file 1
     * <br>
     * <br>
     * @param ssTable SSTable witch record will be created.
     * @return SSTable record.
     */
    private String ssTableIndexRecord(final SSTable ssTable) {
        return String.format("sstable %s %d", ssTable.path().getFileName().toString(), ssTable.priority());
    }

    /**
     * Record for compaction SSTable in index file. Looks like:<br>
     * compaction name priority compacted_file_priority1, compacted_file_priority2...
     * <br>
     * or
     * <br>
     * compaction delete compacted_file_priority1, compacted_file_priority2...
     * <br>
     * Because compacted files can form empty file or no SSTable.
     * <br>
     * Example:
     * <br>
     * compaction delete 1 2 3
     * <br>
     * <br>
     * @param compaction compaction file. Null if compacted in empty SSTable.
     * @param compacted SSTable that were compacted.
     * @return record for compaction file.
     */
    private String compactionIndexRecord(final SSTable compaction, final List<SSTable> compacted) {
        final StringBuilder indexRecord = new StringBuilder("compaction ");
        if (compaction == null) {
            indexRecord.append("delete");
        } else {
            indexRecord.append(compaction.path().getFileName().toString())
                    .append(' ')
                    .append(compaction.priority());
        }
        for (final SSTable ssTable : compacted) {
            indexRecord.append(' ')
                    .append(ssTable.priority());
        }
        return indexRecord.toString();
    }

    /**
     * Adds record in index file.
     * @param line addition line
     * @throws IOException if I/O error occurs.
     */
    private void appendToIndex(final String line) throws IOException {
        indexFileLock.lock();
        try {
            final List<String> newIndexFile = new ArrayList<>(Files.readAllLines(basePath.resolve(indexFileName)));
            newIndexFile.add(line);
            newIndex(newIndexFile);
        } finally {
            indexFileLock.unlock();
        }
    }

    /**
     * Creates new index file from provided list.
     */
    private void newIndex(final List<String> list) throws IOException {
        final Path indexFilePath = basePath.resolve(indexFileName);
        final Path indexFileTmpPath = basePath.resolve(indexFilePath + ".tmp");
        Files.write(
                indexFileTmpPath,
                list,
                StandardOpenOption.CREATE,
                StandardOpenOption.TRUNCATE_EXISTING
        );
        Files.move(
                indexFileTmpPath,
                indexFilePath,
                StandardCopyOption.ATOMIC_MOVE,
                StandardCopyOption.REPLACE_EXISTING
        );
    }

    @Override
    public void close() {
        deleter.close();
        final List<SSTable> currentStorage = storage.get();
        currentStorage.forEach(SSTable::kill);
        while (true) {
            boolean reading = false;
            for (final SSTable ssTable : currentStorage) {
                if (ssTable.readers().get() > 0) {
                    reading = true;
                }
            }
            if (!reading) {
                break;
            }
        }
        if (arena.scope().isAlive()) {
            arena.close();
        }
        storage.set(null);
    }

    @Override
    public Iterator<SSTable> iterator() {
        return storage.get().iterator();
    }
}
