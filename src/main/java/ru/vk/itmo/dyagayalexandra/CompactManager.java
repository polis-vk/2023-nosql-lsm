package ru.vk.itmo.dyagayalexandra;

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
import java.util.Iterator;
import java.util.List;

import static java.nio.file.StandardCopyOption.ATOMIC_MOVE;

public class CompactManager {

    private final String fileName;
    private final String fileExtension;
    private final String fileIndexName;
    private Path compactedFile;
    private Path compactedIndex;
    private final EntryKeyComparator entryKeyComparator;
    private final FileWriterManager fileWriterManager;
    private static final String TEMP_FILE_NAME = "tempCompact.txt";
    private static final String TEMP_FILE_INDEX_NAME = "tempCompactIndex.txt";
    private static final String MAIN_FILE_NAME = "mainCompact.txt";
    private static final String MAIN_FILE_INDEX_NAME = "mainCompactIndex.txt";

    public CompactManager(String fileName, String fileIndexName, String fileExtension,
                          EntryKeyComparator entryKeyComparator, FileWriterManager fileWriterManager) {
        this.fileName = fileName;
        this.fileIndexName = fileIndexName;
        this.fileExtension = fileExtension;
        this.entryKeyComparator = entryKeyComparator;
        this.fileWriterManager = fileWriterManager;
    }

    void compact(Path basePath, FileManager fileManager) throws IOException {
        long indexOffset = Long.BYTES;
        long offset;
        compactedFile = basePath.resolve(TEMP_FILE_NAME);
        compactedIndex = basePath.resolve(TEMP_FILE_INDEX_NAME);

        Iterator<Entry<MemorySegment>> iterator =
                MergedIterator.createMergedIterator(fileManager.createIterators(null, null), entryKeyComparator);

        long storageSize = 0;
        long tableSize = 0;
        while (iterator.hasNext()) {
            Entry<MemorySegment> currentItem = iterator.next();
            tableSize += 2 * Integer.BYTES + currentItem.key().byteSize();
            if (currentItem.value() != null) {
                tableSize += currentItem.value().byteSize();
            }

            storageSize++;
        }

        iterator = MergedIterator.createMergedIterator(fileManager.createIterators(null, null), entryKeyComparator);
        long tableOffset = 0;

        try (FileChannel tableChannel = FileChannel.open(compactedFile, StandardOpenOption.READ,
                StandardOpenOption.WRITE, StandardOpenOption.CREATE);
             FileChannel indexChannel = FileChannel.open(compactedIndex, StandardOpenOption.READ,
                     StandardOpenOption.WRITE, StandardOpenOption.CREATE);
             Arena arena = Arena.ofConfined()) {
            MemorySegment tableMemorySegment = tableChannel.map(FileChannel.MapMode.READ_WRITE,
                    0, tableSize, arena);
            MemorySegment indexMemorySegment = indexChannel.map(FileChannel.MapMode.READ_WRITE,
                    0, (storageSize + 1) * Long.BYTES, arena);
            fileWriterManager.writeStorageSize(indexMemorySegment, storageSize);
            while (iterator.hasNext()) {
                Entry<MemorySegment> currentItem = iterator.next();
                offset = fileWriterManager.writeEntry(tableMemorySegment, tableOffset, currentItem);
                fileWriterManager.writeIndexes(indexMemorySegment, indexOffset, tableOffset);
                tableOffset = offset;
                indexOffset += Long.BYTES;
            }
        }

        Files.move(compactedFile, basePath.resolve(MAIN_FILE_NAME), ATOMIC_MOVE);
        Files.move(compactedIndex, basePath.resolve(MAIN_FILE_INDEX_NAME), ATOMIC_MOVE);
        compactedFile = basePath.resolve(MAIN_FILE_NAME);
        compactedIndex = basePath.resolve(MAIN_FILE_INDEX_NAME);
    }

    void renameCompactFile(Path basePath) throws IOException {
        Files.move(compactedFile, basePath.resolve(fileName + "0" + fileExtension), ATOMIC_MOVE);
        Files.move(compactedIndex, basePath.resolve(fileIndexName + "0" + fileExtension), ATOMIC_MOVE);
    }

    void deleteAllFiles(List<Path> ssTables, List<Path> ssIndexes) {
        for (Path ssTable : ssTables) {
            try {
                Files.deleteIfExists(ssTable);
            } catch (IOException e) {
                throw new UncheckedIOException("Error deleting a ssTable file.", e);
            }
        }

        for (Path ssIndex : ssIndexes) {
            try {
                Files.deleteIfExists(ssIndex);
            } catch (IOException e) {
                throw new UncheckedIOException("Error deleting a ssIndex file.", e);
            }
        }
    }

    boolean deleteTempFile(Path basePath, Path file, List<Path> files) throws IOException {
        if (file.equals(basePath.resolve(TEMP_FILE_NAME))) {
            Files.delete(basePath.resolve(TEMP_FILE_NAME));
            if (files.contains(basePath.resolve(TEMP_FILE_INDEX_NAME))) {
                Files.delete(basePath.resolve(TEMP_FILE_INDEX_NAME));
            }

            return true;
        }

        return false;
    }

    boolean clearIfCompactFileExists(Path basePath, Path file, List<Path> files) throws IOException {
        if (file.equals(basePath.resolve(MAIN_FILE_NAME))) {
            if (files.contains(basePath.resolve(MAIN_FILE_INDEX_NAME))) {
                List<Path> ssTables = new ArrayList<>();
                List<Path> ssIndexes = new ArrayList<>();
                for (Path currentFile : files) {
                    if (currentFile.getFileName().toString().startsWith(fileName)) {
                        ssTables.add(currentFile);
                    }

                    if (currentFile.getFileName().toString().startsWith(fileIndexName)) {
                        ssIndexes.add(currentFile);
                    }
                }

                ssTables.sort(new PathsComparator(fileName, fileExtension));
                ssIndexes.sort(new PathsComparator(fileIndexName, fileExtension));
                deleteFiles(ssTables);
                deleteFiles(ssIndexes);

                Files.move(basePath.resolve(MAIN_FILE_NAME),
                        basePath.resolve(fileName + "0" + fileExtension), ATOMIC_MOVE);
                Files.move(basePath.resolve(MAIN_FILE_INDEX_NAME),
                        basePath.resolve(fileIndexName + "0" + fileExtension), ATOMIC_MOVE);
            } else {
                Files.delete(basePath.resolve(MAIN_FILE_NAME));
            }

            return true;
        }

        return false;
    }

    private void deleteFiles(List<Path> filePaths) throws IOException {
        for (Path filePath : filePaths) {
            Files.delete(filePath);
        }
    }
}
