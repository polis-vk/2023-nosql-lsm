package ru.vk.itmo.shemetovalexey.sstable;

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
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;

public final class SSTable {
    public static final String PREFIX = "data_";
    private static final String TMP_FILE = "index.tmp";
    private static final String IDX_FILE = "index.idx";
    private static final String COMPACTION_TMP = "compaction.tmp";
    private static final String COMPACTION = "compaction";
    private static final int KEY_VALUE_SIZE_OFFSET = 2 * Long.BYTES;

    private SSTable() {
    }

    private static MemorySegment save(
        Arena arena,
        FileChannel fileChannel,
        Iterable<Entry<MemorySegment>> iterable
    ) throws IOException {
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
        long indexSize = count * KEY_VALUE_SIZE_OFFSET;

        MemorySegment fileSegment = fileChannel.map(
            FileChannel.MapMode.READ_WRITE,
            0,
            indexSize + dataSize,
            arena
        );

        long dataOffset = indexSize;
        int indexOffset = 0;
        for (Entry<MemorySegment> entry : iterable) {
            fileSegment.set(ValueLayout.JAVA_LONG_UNALIGNED, indexOffset, dataOffset);
            dataOffset += entry.key().byteSize();
            indexOffset += Long.BYTES;

            MemorySegment value = entry.value();
            if (value == null) {
                fileSegment.set(ValueLayout.JAVA_LONG_UNALIGNED, indexOffset, SSTableUtils.tombstone(dataOffset));
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
        return fileSegment;
    }

    public static MemorySegment saveNextSSTable(
        Arena arena,
        Path storagePath,
        Iterable<Entry<MemorySegment>> iterable
    ) throws IOException {
        final Path indexTmp = storagePath.resolve(TMP_FILE);
        final Path indexFile = storagePath.resolve(IDX_FILE);

        try {
            Files.createFile(indexFile);
        } catch (FileAlreadyExistsException ignored) {
            // ignore
        }
        List<String> existedFiles = Files.readAllLines(indexFile, StandardCharsets.UTF_8);

        String newFileName = PREFIX + existedFiles.size();

        MemorySegment fileSegment;
        try (FileChannel fileChannel = FileChannel.open(
            storagePath.resolve(newFileName),
            StandardOpenOption.WRITE,
            StandardOpenOption.READ,
            StandardOpenOption.CREATE
        )) {
            fileSegment = save(arena, fileChannel, iterable);
        }

        List<String> list = new ArrayList<>(existedFiles.size() + 1);
        list.addAll(existedFiles);
        list.add(newFileName);
        Files.write(
            indexTmp,
            list,
            StandardOpenOption.WRITE,
            StandardOpenOption.CREATE,
            StandardOpenOption.TRUNCATE_EXISTING
        );

        Files.move(indexTmp, indexFile, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
        return fileSegment;
    }

    public static MemorySegment compact(
        Arena arena,
        Path storagePath,
        Iterable<Entry<MemorySegment>> iterable
    ) throws IOException {
        Path compactionTmpFile = storagePath.resolve(COMPACTION_TMP);

        MemorySegment fileSegment;
        try (FileChannel fileChannel = FileChannel.open(
            compactionTmpFile,
            StandardOpenOption.WRITE,
            StandardOpenOption.READ,
            StandardOpenOption.CREATE
        )) {
            fileSegment = save(arena, fileChannel, iterable);
        }

        Files.move(
            compactionTmpFile,
            storagePath.resolve(COMPACTION),
            StandardCopyOption.ATOMIC_MOVE,
            StandardCopyOption.REPLACE_EXISTING
        );

        finalizeCompaction(storagePath);
        return fileSegment;
    }

    private static void finalizeCompaction(Path storagePath) throws IOException {
        try (Stream<Path> stream = Files.find(
            storagePath,
            1,
            (path, attrs) -> path.getFileName().toString().startsWith(PREFIX)
        )) {
            stream.forEach(p -> {
                try {
                    Files.delete(p);
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            });
        }

        Path indexTmp = storagePath.resolve(TMP_FILE);
        Path indexFile = storagePath.resolve(IDX_FILE);

        Files.deleteIfExists(indexFile);
        Files.deleteIfExists(indexTmp);

        Path compactionFile = SSTableUtils.compactionFile(storagePath);
        boolean noData = Files.size(compactionFile) == 0;

        Files.write(
            indexTmp,
            noData ? Collections.emptyList() : Collections.singleton(PREFIX + "0"),
            StandardOpenOption.WRITE,
            StandardOpenOption.CREATE,
            StandardOpenOption.TRUNCATE_EXISTING
        );

        Files.move(indexTmp, indexFile, StandardCopyOption.ATOMIC_MOVE);
        if (noData) {
            Files.delete(compactionFile);
        } else {
            Files.move(compactionFile, storagePath.resolve(PREFIX + "0"), StandardCopyOption.ATOMIC_MOVE);
        }
    }

    public static List<MemorySegment> loadOrRecover(Path storagePath, Arena arena) throws IOException {
        if (Files.exists(SSTableUtils.compactionFile(storagePath))) {
            finalizeCompaction(storagePath);
        }

        Path indexTmp = storagePath.resolve(TMP_FILE);
        Path indexFile = storagePath.resolve(IDX_FILE);

        if (!Files.exists(indexFile)) {
            if (Files.exists(indexTmp)) {
                Files.move(indexTmp, indexFile, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
            } else {
                Files.createFile(indexFile);
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
}
