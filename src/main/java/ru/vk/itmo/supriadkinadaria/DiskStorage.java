package ru.vk.itmo.supriadkinadaria;

import ru.vk.itmo.Entry;

import java.io.File;
import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

import static ru.vk.itmo.supriadkinadaria.DiskStorageUtils.tombstone;

public class DiskStorage {

    private static final String INDEX_FILE = "index.idx";
    private static final String INDEX_TMP_FILE = "index.tmp";
    private final List<MemorySegment> segmentList;

    public DiskStorage(List<MemorySegment> segmentList) {
        this.segmentList = segmentList;
    }

    public static void saveData(Path newFilePath, Iterable<Entry<MemorySegment>> iterable) throws IOException {
        long dataSize = 0;
        long count = 0;
        for (Entry<MemorySegment> entry : iterable) {
            dataSize += entry.key().byteSize();
            MemorySegment value = entry.value();
            if (value != null) {
                dataSize += value.byteSize();
            }
            count++;
        }
        long indexSize = count * 2 * Long.BYTES;

        try (
                FileChannel fileChannel = FileChannel.open(
                        newFilePath,
                        StandardOpenOption.WRITE,
                        StandardOpenOption.READ,
                        StandardOpenOption.CREATE
                );
                Arena writeArena = Arena.ofConfined()
        ) {
            MemorySegment fileSegment = fileChannel.map(
                    FileChannel.MapMode.READ_WRITE,
                    0,
                    indexSize + dataSize,
                    writeArena
            );

            long dataOffset = indexSize;
            int indexOffset = 0;
            for (Entry<MemorySegment> entry : iterable) {
                fileSegment.set(ValueLayout.JAVA_LONG_UNALIGNED, indexOffset, dataOffset);
                dataOffset += entry.key().byteSize();
                indexOffset += Long.BYTES;

                MemorySegment value = entry.value();
                if (value == null) {
                    fileSegment.set(ValueLayout.JAVA_LONG_UNALIGNED, indexOffset, tombstone(dataOffset));
                } else {
                    fileSegment.set(ValueLayout.JAVA_LONG_UNALIGNED, indexOffset, dataOffset);
                    dataOffset += value.byteSize();
                }
                indexOffset += Long.BYTES;
            }

            dataOffset = indexSize;
            for (Entry<MemorySegment> entry : iterable) {
                MemorySegment key = entry.key();
                MemorySegment.copy(key, 0, fileSegment, dataOffset, key.byteSize());
                dataOffset += key.byteSize();

                MemorySegment value = entry.value();
                if (value != null) {
                    MemorySegment.copy(value, 0, fileSegment, dataOffset, value.byteSize());
                    dataOffset += value.byteSize();
                }
            }
        }
    }

    private static void rewriteIndexFile(Path indexFile, Path indexTmp, List<String> list) throws IOException {
        Files.move(indexFile, indexTmp, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);

        Files.write(
                indexFile,
                list,
                StandardOpenOption.WRITE,
                StandardOpenOption.CREATE,
                StandardOpenOption.TRUNCATE_EXISTING
        );

        Files.delete(indexTmp);
    }

    public static void save(Path storagePath, Iterable<Entry<MemorySegment>> iterable)
            throws IOException {
        final Path indexTmp = storagePath.resolve(INDEX_TMP_FILE);
        final Path indexFile = storagePath.resolve(INDEX_FILE);

        try {
            Files.createFile(indexFile);
        } catch (FileAlreadyExistsException ignored) {
            // it is ok, actually it is normal state
        }
        List<String> existedFiles = Files.readAllLines(indexFile, StandardCharsets.UTF_8);

        String newFileName = getFileNameAccordingToIndexFileSize(indexFile);

        saveData(storagePath.resolve(newFileName), iterable);

        List<String> list = new ArrayList<>(existedFiles.size() + 1);
        list.addAll(existedFiles);
        list.add(newFileName);

        rewriteIndexFile(indexFile, indexTmp, list);
    }

    private static String getFileNameAccordingToIndexFileSize(Path indexFile) throws IOException {
        List<String> existedFiles = Files.readAllLines(indexFile, StandardCharsets.UTF_8);
        return String.valueOf(existedFiles.size());
    }

    public static List<MemorySegment> loadOrRecover(Path storagePath, Arena arena) throws IOException {
        Path indexTmp = storagePath.resolve(INDEX_TMP_FILE);
        Path indexFile = storagePath.resolve(INDEX_FILE);

        if (Files.exists(indexTmp)) {
            Files.move(indexTmp, indexFile, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
        } else {
            try {
                Files.createFile(indexFile);
            } catch (FileAlreadyExistsException ignored) {
                // it is ok, actually it is normal state
            }
        }

        List<String> existedFiles = Files.readAllLines(indexFile, StandardCharsets.UTF_8);
        List<MemorySegment> result = new ArrayList<>(existedFiles.size());
        for (String fileName : existedFiles) {
            Path file = storagePath.resolve(fileName);
            try (FileChannel fileChannel = FileChannel.open(file, StandardOpenOption.READ, StandardOpenOption.WRITE)) {
                MemorySegment fileSegment = fileChannel.map(
                        FileChannel.MapMode.READ_WRITE,
                        0,
                        Files.size(file),
                        arena
                );
                result.add(fileSegment);
            }
        }

        return result;
    }

    public Iterator<Entry<MemorySegment>> range(
            Iterator<Entry<MemorySegment>> firstIterator,
            MemorySegment from,
            MemorySegment to) {
        return DiskStorageUtils.range(firstIterator, from, to, segmentList);
    }

    public void compact(Path storagePath, Iterable<Entry<MemorySegment>> iterable) throws IOException {
        Path indexFile = storagePath.resolve(INDEX_FILE);
        Path indexTmp = storagePath.resolve(INDEX_TMP_FILE);
        Path newFilePath = storagePath.resolve(getFileNameAccordingToIndexFileSize(indexFile));
        saveData(newFilePath, iterable);
        for (File file : Objects.requireNonNull(storagePath.toFile().listFiles())) {
            if (!file.isDirectory()
                    && !(file.getName().equals(INDEX_FILE)
                    || file.getName().equals(getFileNameAccordingToIndexFileSize(indexFile)))) {
                Files.delete(file.toPath());
            }
        }

        if (newFilePath.toFile().renameTo(storagePath.resolve("0").toFile())) {
            rewriteIndexFile(indexFile, indexTmp, List.of("0"));
        }
    }
}
