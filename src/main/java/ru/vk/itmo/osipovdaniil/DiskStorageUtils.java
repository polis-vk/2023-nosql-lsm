package ru.vk.itmo.osipovdaniil;

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

public final class DiskStorageUtils {

    public static final String SSTABLE_PREFIX = "sstable_";

    private DiskStorageUtils() {
    }

    public static List<MemorySegment> loadOrRecover(final Path storagePath, final Arena arena) throws IOException {
        if (Files.exists(compactionFile(storagePath))) {
            finalizeCompaction(storagePath);
        }
        final Path indexTmp = DiskStorageUtilsSimple.getIndexTmpPath(storagePath);
        final Path indexFile = DiskStorageUtilsSimple.getIndexPath(storagePath);
        if (!Files.exists(indexFile)) {
            if (Files.exists(indexTmp)) {
                Files.move(indexTmp, indexFile, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
            } else {
                Files.createFile(indexFile);
            }
        }
        final List<String> existedFiles = Files.readAllLines(indexFile, StandardCharsets.UTF_8);
        final List<MemorySegment> result = new ArrayList<>(existedFiles.size());
        for (String fileName : existedFiles) {
            final Path file = storagePath.resolve(fileName);
            try (FileChannel fileChannel = FileChannel.open(file, StandardOpenOption.READ, StandardOpenOption.WRITE)) {
                MemorySegment fileSegment = fileChannel.map(
                        FileChannel.MapMode.READ_WRITE,
                        0,
                        Files.size(file),
                        arena);
                result.add(fileSegment);
            }
        }
        return result;
    }

    static long indexOf(final MemorySegment segment, final MemorySegment key) {
        final long recordsCount = DiskStorageUtilsSimple.recordsCount(segment);

        long left = 0;
        long right = recordsCount - 1;
        while (left <= right) {
            long mid = (left + right) >>> 1;
            long startOfKey = DiskStorageUtilsSimple.startOfKey(segment, mid);
            long endOfKey = DiskStorageUtilsSimple.endOfKey(segment, mid);
            long mismatch = MemorySegment.mismatch(segment, startOfKey, endOfKey, key, 0, key.byteSize());
            if (mismatch == -1) {
                return mid;
            }
            if (mismatch == key.byteSize()) {
                right = mid - 1;
                continue;
            }
            if (mismatch == endOfKey - startOfKey) {
                left = mid + 1;
                continue;
            }
            int b1 = Byte.toUnsignedInt(segment.get(ValueLayout.JAVA_BYTE, startOfKey + mismatch));
            int b2 = Byte.toUnsignedInt(key.get(ValueLayout.JAVA_BYTE, mismatch));
            if (b1 > b2) {
                right = mid - 1;
            } else {
                left = mid + 1;
            }
        }

        return DiskStorageUtilsSimple.tombstone(left);
    }

    public static void save(final Path storagePath, final Iterable<Entry<MemorySegment>> iterable)
            throws IOException {
        final Path indexTmp = DiskStorageUtilsSimple.getIndexTmpPath(storagePath);
        final Path indexFile = DiskStorageUtilsSimple.getIndexPath(storagePath);

        try {
            Files.createFile(indexFile);
        } catch (FileAlreadyExistsException ignored) {
            // it is ok, actually it is normal state
        }
        final List<String> existedFiles = Files.readAllLines(indexFile, StandardCharsets.UTF_8);
        final String newFileName = SSTABLE_PREFIX + existedFiles.size();
        final Path newFilePath = storagePath.resolve(newFileName);
        dump(iterable, newFilePath);
        final List<String> list = new ArrayList<>(existedFiles.size() + 1);
        list.addAll(existedFiles);
        list.add(newFileName);
        Files.write(indexTmp,
                list,
                StandardOpenOption.WRITE,
                StandardOpenOption.CREATE,
                StandardOpenOption.TRUNCATE_EXISTING);
        Files.deleteIfExists(indexFile);
        Files.move(indexTmp, indexFile, StandardCopyOption.ATOMIC_MOVE);
    }

    private static void dump(final Iterable<Entry<MemorySegment>> iterable,
                             final Path newFilePath) throws IOException {
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
        final long indexSize = count * 2 * Long.BYTES;
        try (FileChannel fileChannel = FileChannel.open(
                newFilePath,
                StandardOpenOption.WRITE,
                StandardOpenOption.READ,
                StandardOpenOption.CREATE);
             Arena writeArena = Arena.ofConfined()) {
            MemorySegment fileSegment = fileChannel.map(
                    FileChannel.MapMode.READ_WRITE,
                    0,
                    indexSize + dataSize,
                    writeArena
            );
            // index:
            // |key0_Start|value0_Start|key1_Start|value1_Start|key2_Start|value2_Start|...
            // key0_Start = data start = end of index
            long dataOffset = indexSize;
            int indexOffset = 0;
            for (Entry<MemorySegment> entry : iterable) {
                fileSegment.set(ValueLayout.JAVA_LONG_UNALIGNED, indexOffset, dataOffset);
                dataOffset += entry.key().byteSize();
                indexOffset += Long.BYTES;

                MemorySegment value = entry.value();
                if (value == null) {
                    fileSegment.set(ValueLayout.JAVA_LONG_UNALIGNED, indexOffset,
                            DiskStorageUtilsSimple.tombstone(dataOffset));
                } else {
                    fileSegment.set(ValueLayout.JAVA_LONG_UNALIGNED, indexOffset, dataOffset);
                    dataOffset += value.byteSize();
                }
                indexOffset += Long.BYTES;
            }
            // data:
            // |key0|value0|key1|value1|...
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

    public static void compact(final Path storagePath, final Iterable<Entry<MemorySegment>> iterable)
            throws IOException {
        final String newFileName = "compaction.tmp";
        final Path compactionTmpFile = storagePath.resolve(newFileName);
        final Path newFilePath = storagePath.resolve(newFileName);
        dump(iterable, newFilePath);
        Files.move(compactionTmpFile,
                storagePath.resolve("compaction"),
                StandardCopyOption.ATOMIC_MOVE,
                StandardCopyOption.REPLACE_EXISTING);
        finalizeCompaction(storagePath);
    }

    private static void finalizeCompaction(Path storagePath) throws IOException {
        final Path compactionFile = compactionFile(storagePath);
        try (Stream<Path> stream = Files.find(storagePath, 1,
                (path, attr) -> path.getFileName().toString().startsWith(SSTABLE_PREFIX))) {
            stream.forEach(p -> {
                try {
                    Files.delete(p);
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            });
        }

        final Path indexTmp = storagePath.resolve("index.tmp");
        final Path indexFile = storagePath.resolve("index.idx");

        Files.deleteIfExists(indexFile);
        Files.deleteIfExists(indexTmp);

        boolean noData = Files.size(compactionFile) == 0;

        Files.write(indexTmp,
                noData ? Collections.emptyList() : Collections.singleton(SSTABLE_PREFIX + "0"),
                StandardOpenOption.WRITE,
                StandardOpenOption.CREATE,
                StandardOpenOption.TRUNCATE_EXISTING);
        Files.move(indexTmp, indexFile, StandardCopyOption.ATOMIC_MOVE);
        if (noData) {
            Files.delete(compactionFile);
        } else {
            Files.move(compactionFile, storagePath.resolve(SSTABLE_PREFIX + "0"), StandardCopyOption.ATOMIC_MOVE);
        }
    }

    private static Path compactionFile(Path storagePath) {
        return storagePath.resolve("compaction");
    }
}
