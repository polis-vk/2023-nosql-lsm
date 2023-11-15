package ru.vk.itmo.smirnovdmitrii.outofmemory.sstable;

import ru.vk.itmo.smirnovdmitrii.outofmemory.IndexFileRecord;

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
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

public class SSTableStorageImpl implements SSTableStorage {
    private final ConcurrentSkipListSet<SSTable> storage = new ConcurrentSkipListSet<>();
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
        for (final String indexRecord: lines) {
            final IndexFileRecord indexFileRecord = new IndexFileRecord(indexRecord);
            final long priority = indexFileRecord.getPriority();
            maxPriority = Math.max(maxPriority, priority);
            if (compacted.remove(priority)) {
                continue;
            }

            if (!indexFileRecord.getName().equals("delete")) {
                final Path tablePath = basePath.resolve(indexFileRecord.getName());
                if (Files.exists(tablePath)) {
                    try (FileChannel channel = FileChannel.open(tablePath, StandardOpenOption.READ)) {
                        storage.add(
                                new SSTable(
                                        channel.map(FileChannel.MapMode.READ_ONLY, 0, channel.size(), arena),
                                        tablePath,
                                        priority
                                )
                        );
                    }
                    ssTableNames.add(indexFileRecord.getName());
                    if (indexFileRecord.isCompaction()) {
                        compacted.addAll(indexFileRecord.getCompactedPriorities());
                    }
                }
            }
        }
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
        newIndex(storage.stream()
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
        storage.add(ssTable);
    }

    @Override
    public SSTable getLast() {
        return storage.getLast();
    }

    @Override
    public void compact(final String compactionFileName, final List<SSTable> compacted) throws IOException {
        if (compactionFileName != null) {
            final Path compactionPath = basePath.resolve(compactionFileName);
            final MemorySegment mappedCompaction = map(compactionPath);
            long minPriority = Long.MAX_VALUE;
            for (final SSTable ssTable : compacted) {
                minPriority = Math.min(ssTable.priority(), minPriority);
            }
            final SSTable compaction = new SSTable(mappedCompaction, compactionPath, minPriority);
            appendToIndex(compactionIndexRecord(compaction, compacted));
        }
        compacted.forEach(storage::remove);
        deleter.execute(() -> deleteTask(
                compacted.stream()
                        .filter(ssTable -> ssTable.isAlive().compareAndSet(true, false))
                        .collect(Collectors.toCollection(ArrayList::new)))
        );
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
                    .append(" ")
                    .append(compaction.priority());
        }
        for (final SSTable ssTable: compacted) {
            indexRecord.append(" ")
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
        storage.forEach(it -> it.isAlive().set(false));
        while (true) {
            boolean reading = false;
            for (final SSTable ssTable: storage) {
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
    }

    @Override
    public Iterator<SSTable> iterator() {
        return storage.iterator();
    }
}
