package ru.vk.itmo.chernyshevyaroslav;

import ru.vk.itmo.Entry;

import java.io.IOException;
import java.io.UncheckedIOException;
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

public final class FileUtils {

    private static final String INDEX_IDX = "index.idx";
    private static final String INDEX_TMP = "index.tmp";
    private static final String COMPACTION_TMP = "Compaction.tmp";

    private FileUtils() {
        throw new IllegalStateException("Utility class");
    }

    public static void save(Path storagePath, Iterable<Entry<MemorySegment>> iterable, boolean isCompaction) {
        final Path indexTmp = storagePath.resolve(INDEX_TMP);
        final Path indexFile = storagePath.resolve(INDEX_IDX);

        try {
            Files.createFile(indexFile);
        } catch (FileAlreadyExistsException ignored) {
            // it is ok, actually it is normal state
        } catch (IOException e) {
            throw new UncheckedIOException("parent directory does not exist", e);
        }
        List<String> existedFiles;
        try {
            existedFiles = Files.readAllLines(indexFile, StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        String newFileName = isCompaction ? COMPACTION_TMP : String.valueOf(existedFiles.size());

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

        MemorySegment fileSegment;
        Arena writeArena = Arena.ofConfined();
        try (
                FileChannel fileChannel = FileChannel.open(
                        storagePath.resolve(newFileName),
                        StandardOpenOption.WRITE,
                        StandardOpenOption.READ,
                        StandardOpenOption.CREATE
                )

        ) {
            fileSegment = fileChannel.map(
                    FileChannel.MapMode.READ_WRITE,
                    0,
                    indexSize + dataSize,
                    writeArena
            );
        } catch (IOException e) {
            throw new RuntimeException("fileSegment IOException");
        }
        // index:
        // |key0_Start|value0_Start|key1_Start|value1_Start|key2_Start|value2_Start|...
        // key0_Start = data start = end of index
        long dataOffset = indexSize;
        int indexOffset = 0;
        Iterator<Entry<MemorySegment>> localIterator = iterable.iterator();
        for (int i = 0; i < count; i++) {
            Entry<MemorySegment> entry = localIterator.next();
            fileSegment.set(ValueLayout.JAVA_LONG_UNALIGNED, indexOffset, dataOffset);
            dataOffset += entry.key().byteSize();
            indexOffset += Long.BYTES;

            MemorySegment value = entry.value();
            if (value == null) {
                fileSegment.set(ValueLayout.JAVA_LONG_UNALIGNED, indexOffset, DiskStorage.tombstone(dataOffset));
            } else {
                fileSegment.set(ValueLayout.JAVA_LONG_UNALIGNED, indexOffset, dataOffset);
                dataOffset += value.byteSize();
            }
            indexOffset += Long.BYTES;
        }

        // data:
        // |key0|value0|key1|value1|...
        dataOffset = indexSize;
        localIterator = iterable.iterator();
        for (int i = 0; i < count; i++) {
            Entry<MemorySegment> entry = localIterator.next();
            MemorySegment key = entry.key();
            MemorySegment.copy(key, 0, fileSegment, dataOffset, key.byteSize());
            dataOffset += key.byteSize();

            MemorySegment value = entry.value();
            if (value != null) {
                MemorySegment.copy(value, 0, fileSegment, dataOffset, value.byteSize());
                dataOffset += value.byteSize();
            }
        }

        try {
            Files.move(indexFile, indexTmp, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        List<String> list = new ArrayList<>(existedFiles.size() + 1);
        list.addAll(existedFiles);
        if (!isCompaction) {
            list.add(newFileName);
        }
        try {
            Files.write(
                    indexFile,
                    list,
                    StandardOpenOption.WRITE,
                    StandardOpenOption.CREATE,
                    StandardOpenOption.TRUNCATE_EXISTING
            );
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        try {
            Files.delete(indexTmp);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static List<MemorySegment> loadOrRecover(Path storagePath, Arena arena) throws IOException {
        if (Files.exists(storagePath.resolve(COMPACTION_TMP))) {
            finalizeCompaction(storagePath);
        }
        Path indexTmp = storagePath.resolve(INDEX_TMP);
        Path indexFile = storagePath.resolve(INDEX_IDX);

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

    public static void compact(Path storagePath, Iterable<Entry<MemorySegment>> iterable) {
        Path indexFile = storagePath.resolve(INDEX_IDX);

        try {
            Files.createFile(indexFile);
        } catch (FileAlreadyExistsException ignored) {
            // it is ok, actually it is normal state
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        List<String> existingFiles;
        try {
            existingFiles = Files.readAllLines(indexFile, StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        if (existingFiles.isEmpty()) {
            try {
                Files.delete(indexFile);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            return;
        }
        save(storagePath, iterable, true);
        finalizeCompaction(storagePath);
    }

    private static void finalizeCompaction(Path storagePath) {
        Path indexFile = storagePath.resolve(INDEX_IDX);

        try {
            List<String> existingFiles = Files.readAllLines(indexFile, StandardCharsets.UTF_8);

            for (String file : existingFiles) {
                Files.deleteIfExists(storagePath.resolve(file));
            }

            Files.writeString(
                    indexFile,
                    "0",
                    StandardOpenOption.WRITE,
                    StandardOpenOption.CREATE,
                    StandardOpenOption.TRUNCATE_EXISTING
            );

            Files.move(storagePath.resolve(COMPACTION_TMP),
                    storagePath.resolve("0"),
                    StandardCopyOption.ATOMIC_MOVE,
                    StandardCopyOption.REPLACE_EXISTING);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
