package ru.vk.itmo.smirnovdmitrii.outofmemory.sstable;

import ru.vk.itmo.smirnovdmitrii.outofmemory.IndexFileRecord;
import ru.vk.itmo.smirnovdmitrii.util.exceptions.CorruptedError;

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
import java.util.Collections;
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

public class SSTableStorageImpl implements SSTableStorage {
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

    private void load() throws IOException {
        final List<String> lines = Files.readAllLines(basePath.resolve(indexFileName)).reversed();
        final Set<Long> compacted = new HashSet<>();
        final List<String> ssTableNames = new ArrayList<>();
        long maxPriority = 0;
        final List<SSTable> ssTables = new ArrayList<>();
        for (final String indexRecord : lines) {
            final IndexFileRecord indexFileRecord = new IndexFileRecord(indexRecord);
            final long priority = indexFileRecord.getPriority();
            maxPriority = Math.max(maxPriority, priority);
            if (!compacted.remove(priority) && !indexFileRecord.getName().equals("delete")) {
                final Path tablePath = basePath.resolve(indexFileRecord.getName());
                if (Files.notExists(tablePath)) {
                    throw new CorruptedError("corrupted: file from record [" + indexRecord + "] not found.");
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
        Collections.sort(ssTables);
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
        newIndex(ssTables.stream()
                .map(this::ssTableIndexRecord)
                .toList()
        );
    }

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

    @Override
    public SSTable getCompaction(final SSTable ssTable) {
        final List<SSTable> currentStorage = storage.get();
        if (currentStorage.isEmpty()) {
            return null;
        }
        return currentStorage.getLast();
    }

    @Override
    public void compact(final String compactionFileName, final List<SSTable> compacted) throws IOException {
        SSTable compaction = null;
        if (compactionFileName != null) {
            final Path compactionPath = basePath.resolve(compactionFileName);
            final MemorySegment mappedCompaction = map(compactionPath);
            long minPriority = Long.MAX_VALUE;
            for (final SSTable ssTable : compacted) {
                minPriority = Math.min(ssTable.priority(), minPriority);
            }
            compaction = new SSTable(mappedCompaction, compactionPath, minPriority);
            appendToIndex(compactionIndexRecord(compaction, compacted));
        }
        final SSTable firstCompacted = compacted.getFirst();
        while (true) {
            final List<SSTable> oldSSTables = storage.get();
            final List<SSTable> newSSTables = new ArrayList<>();
            int index = 0;
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

    private String ssTableIndexRecord(final SSTable ssTable) {
        return String.format("sstable %s %d", ssTable.path().getFileName().toString(), ssTable.priority());
    }

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
