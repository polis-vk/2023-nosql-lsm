package ru.vk.itmo.dyagayalexandra;

import ru.vk.itmo.Config;
import ru.vk.itmo.Entry;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class FileManager {
    private static final String FILE_NAME = "data";
    private static final String FILE_INDEX_NAME = "index";
    private static final String FILE_EXTENSION = ".txt";
    private final Path basePath;
    private final List<MemorySegment> ssTables;
    private final List<MemorySegment> ssIndexes;
    private final List<Path> ssTablesPaths;
    private final List<Path> ssIndexesPaths;
    private final CompactManager compactManager;
    private final FileWriterManager fileWriterManager;
    private final FileReaderManager fileReaderManager;
    private final MemorySegmentComparator memorySegmentComparator;
    private final Arena arena;
    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    public FileManager(Config config, MemorySegmentComparator memorySegmentComparator,
                       EntryKeyComparator entryKeyComparator) {
        basePath = config.basePath();
        ssTables = new ArrayList<>();
        ssIndexes = new ArrayList<>();
        ssTablesPaths = new ArrayList<>();
        ssIndexesPaths = new ArrayList<>();
        arena = Arena.ofShared();
        this.memorySegmentComparator = memorySegmentComparator;
        fileWriterManager = new FileWriterManager(FILE_NAME, FILE_EXTENSION, FILE_INDEX_NAME, basePath, arena);
        fileReaderManager = new FileReaderManager(memorySegmentComparator);
        compactManager = new CompactManager(FILE_NAME, FILE_INDEX_NAME, FILE_EXTENSION, entryKeyComparator,
                                                                                      fileWriterManager);
        FileChecker fileChecker = new FileChecker(FILE_NAME, FILE_INDEX_NAME, FILE_EXTENSION, compactManager);
        try {
            List<Map.Entry<MemorySegment, MemorySegment>> allDataSegments = fileChecker.checkFiles(basePath, arena);
            getData(allDataSegments, fileChecker.getAllDataPaths(basePath));
        } catch (IOException e) {
            throw new UncheckedIOException("Error checking files.", e);
        }
    }

    Entry<MemorySegment> get(MemorySegment key) {
        for (int i = 0; i < ssTables.size(); i++) {
            FileIterator fileIterator;
            try {
                fileIterator = new FileIterator(ssTables.get(i), ssIndexes.get(i), key, null,
                        fileReaderManager.getIndexSize(ssIndexes.get(i)), fileReaderManager);
            } catch (IOException e) {
                throw new UncheckedIOException("Failed to create FileIterator", e);
            }

            if (fileIterator.hasNext()) {
                Entry<MemorySegment> currentEntry = fileIterator.next();
                if (currentEntry != null && memorySegmentComparator.compare(currentEntry.key(), key) == 0) {
                    return currentEntry;
                }
            }
        }

        return null;
    }

    void performCompact() {
        if (ssTables.size() <= 1) {
            return;
        }

        try {
            compactManager.compact(basePath, this);
            compactManager.deleteAllFiles(ssTablesPaths, ssIndexesPaths);
            compactManager.renameCompactFile(basePath);
            afterCompact();
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to complete compact.", e);
        }
    }

    void flush(Collection<Entry<MemorySegment>> entryCollection) {
        try {
            FileWriterManager.SavedFilesInfo savedFilesInfo =
                    fileWriterManager.save(entryCollection, ssTables.size(), ssIndexes.size());
            lock.readLock().lock();
            try {
                ssTables.addFirst(savedFilesInfo.getSSTable());
                ssIndexes.addFirst(savedFilesInfo.getSSIndex());
                ssTablesPaths.addFirst(savedFilesInfo.getSSTablePath());
                ssIndexesPaths.addFirst(savedFilesInfo.getSSIndexPath());
            } finally {
                lock.readLock().unlock();
            }
        } catch (IOException e) {
            throw new UncheckedIOException("Error saving storage.", e);
        }
    }

    List<Iterator<Entry<MemorySegment>>> createIterators(MemorySegment from, MemorySegment to) {
        List<Iterator<Entry<MemorySegment>>> iterators = new ArrayList<>();
        for (int i = 0; i < ssTables.size(); i++) {
            iterators.add(createFileIterator(ssTables.get(i), ssIndexes.get(i), from, to));
        }

        return iterators;
    }

    private Iterator<Entry<MemorySegment>> createFileIterator(MemorySegment ssTable, MemorySegment ssIndex,
                                                              MemorySegment from, MemorySegment to) {
        try {
            long indexSize = fileReaderManager.getIndexSize(ssIndex);
            return new FileIterator(ssTable, ssIndex, from, to, indexSize, fileReaderManager);
        } catch (IOException e) {
            throw new UncheckedIOException("An error occurred while reading files.", e);
        }
    }

    private void getData(List<Map.Entry<MemorySegment, MemorySegment>> allDataSegments,
                         List<Map.Entry<Path, Path>> allDataPaths) {
        for (Map.Entry<MemorySegment, MemorySegment> entry : allDataSegments) {
            ssTables.add(entry.getKey());
            ssIndexes.add(entry.getValue());
        }

        for (Map.Entry<Path, Path> entry : allDataPaths) {
            ssTablesPaths.add(entry.getKey());
            ssIndexesPaths.add(entry.getValue());
        }
    }

    private void afterCompact() throws IOException {
        Path ssTablePath = basePath.resolve(FILE_NAME + "0" + FILE_EXTENSION);
        Path ssIndexPath = basePath.resolve(FILE_INDEX_NAME + "0" + FILE_EXTENSION);

        MemorySegment tableMemorySegment;
        MemorySegment indexMemorySegment;

        try (FileChannel tableChannel = FileChannel.open(ssTablePath, StandardOpenOption.READ,
                StandardOpenOption.WRITE, StandardOpenOption.CREATE)) {
            tableMemorySegment = tableChannel.map(FileChannel.MapMode.READ_WRITE,
                    0, Files.size(ssTablePath), arena);
        }

        try (FileChannel indexChannel = FileChannel.open(ssIndexPath, StandardOpenOption.READ,
                StandardOpenOption.WRITE, StandardOpenOption.CREATE)) {
            indexMemorySegment = indexChannel.map(FileChannel.MapMode.READ_WRITE,
                    0, Files.size(ssIndexPath), arena);
        }

        lock.readLock().lock();
        try {
            ssTables.clear();
            ssIndexes.clear();
            ssTablesPaths.clear();
            ssIndexesPaths.clear();
            ssTables.add(tableMemorySegment);
            ssIndexes.add(indexMemorySegment);
            ssTablesPaths.add(ssTablePath);
            ssIndexesPaths.add(ssIndexPath);
        } finally {
            lock.readLock().unlock();
        }
    }

    void closeArena() {
        if (arena == null || !arena.scope().isAlive()) {
            return;
        }
        arena.close();
    }
}
