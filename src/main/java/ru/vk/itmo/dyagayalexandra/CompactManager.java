package ru.vk.itmo.dyagayalexandra;

import ru.vk.itmo.BaseEntry;
import ru.vk.itmo.Entry;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static java.nio.file.StandardCopyOption.ATOMIC_MOVE;

public class CompactManager {

    private final String fileName;
    private final String fileExtension;
    private final String fileIndexName;
    private Path compactedFile;
    private Path compactedIndex;
    private static final String TEMP_FILE_NAME = "tempCompact.txt";
    private static final String TEMP_FILE_INDEX_NAME = "tempCompactIndex.txt";
    private static final String MAIN_FILE_NAME = "mainCompact.txt";
    private static final String MAIN_FILE_INDEX_NAME = "mainCompactIndex.txt";

    public CompactManager(String fileName, String fileIndexName, String fileExtension) {
        this.fileName = fileName;
        this.fileIndexName = fileIndexName;
        this.fileExtension = fileExtension;
    }

    public void compact(Path basePath, Iterator<Entry<MemorySegment>> iterator) throws IOException {
        long count = 0;
        long offset = 0;
        compactedFile = basePath.resolve(TEMP_FILE_NAME);
        compactedIndex = basePath.resolve(TEMP_FILE_INDEX_NAME);
        Files.createFile(compactedFile);
        Files.createFile(compactedIndex);


        try (FileChannelsHandler writer = new FileChannelsHandler(compactedFile, compactedIndex)) {
            getStartPosition(writer.getIndexChannel());
            while (iterator.hasNext()) {
                Entry<MemorySegment> currentItem = iterator.next();
                Map.Entry<MemorySegment, Entry<MemorySegment>> currentEntry =
                        Map.entry(currentItem.key(), new BaseEntry<>(currentItem.key(), currentItem.value()));
                FileManager.writeEntry(writer.getFileChannel(), currentEntry);
                offset = FileManager.writeIndexes(writer.getIndexChannel(), offset, currentEntry);
                count++;
            }

            setIndexSize(writer.getIndexChannel(), count);
        }

        Files.move(compactedFile, basePath.resolve(MAIN_FILE_NAME), ATOMIC_MOVE);
        Files.move(compactedIndex, basePath.resolve(MAIN_FILE_INDEX_NAME), ATOMIC_MOVE);
        compactedFile = basePath.resolve(MAIN_FILE_NAME);
        compactedIndex = basePath.resolve(MAIN_FILE_INDEX_NAME);
    }

    public void renameCompactFile(Path basePath) throws IOException {
        Files.move(compactedFile, basePath.resolve(fileName + "0" + fileExtension), ATOMIC_MOVE);
        Files.move(compactedIndex, basePath.resolve(fileIndexName + "0" + fileExtension), ATOMIC_MOVE);
    }

    public void deleteAllFiles(List<Path> ssTables, List<Path> ssIndexes) {
        for (Path ssTable : ssTables) {
            try {
                Files.delete(ssTable);
            } catch (IOException e) {
                throw new UncheckedIOException("Error deleting a ssTable file.", e);
            }
        }

        for (Path ssIndex : ssIndexes) {
            try {
                Files.delete(ssIndex);
            } catch (IOException e) {
                throw new UncheckedIOException("Error deleting a ssIndex file.", e);
            }
        }
    }

    public boolean deleteTempFile(Path basePath, Path file, List<Path> files) throws IOException {
        if (file.equals(basePath.resolve(TEMP_FILE_NAME))) {
            Files.delete(basePath.resolve(TEMP_FILE_NAME));
            if (files.contains(basePath.resolve(TEMP_FILE_INDEX_NAME))) {
                Files.delete(basePath.resolve(TEMP_FILE_INDEX_NAME));
            }

            return true;
        }

        return false;
    }

    public boolean clearIfCompactFileExists(Path basePath, Path file, List<Path> files) throws IOException {
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

    private static void getStartPosition(FileChannel fileChannel) throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
        fileChannel.position(Long.BYTES);
        buffer.putLong(0);
        buffer.flip();
        fileChannel.write(buffer);
    }

    private static void setIndexSize(FileChannel fileChannel, long count) throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
        fileChannel.position(0);
        buffer.putLong(count);
        buffer.flip();
        fileChannel.write(buffer);
    }

    private void deleteFiles(List<Path> filePaths) throws IOException {
        for (Path filePath : filePaths) {
            Files.delete(filePath);
        }
    }
}
