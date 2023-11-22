package ru.vk.itmo.volkovnikita;

import ru.vk.itmo.Entry;

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
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Stream;

import static ru.vk.itmo.volkovnikita.Utils.iterator;

public class DiskStorage {
    private final List<MemorySegment> segmentList = new CopyOnWriteArrayList<>();

    public DiskStorage(List<MemorySegment> segmentList) {
        this.segmentList.addAll(segmentList);
    }

    public Iterator<Entry<MemorySegment>> range(
            MemorySegment from,
            MemorySegment to,
            List<Iterator<Entry<MemorySegment>>> addIterators) {
        List<Iterator<Entry<MemorySegment>>> iterators = new ArrayList<>(segmentList.size() + 1);
        for (MemorySegment memorySegment : segmentList) {
            iterators.add(iterator(memorySegment, from, to));
        }
        iterators.addAll(addIterators);

        return new MergeIterator<>(iterators, Comparator.comparing(Entry::key, DaoImpl::compare)) {
            @Override
            protected boolean skip(Entry<MemorySegment> memorySegmentEntry) {
                return memorySegmentEntry.value() == null;
            }
        };
    }

    public void save(Path storagePath, Iterable<Entry<MemorySegment>> iterable, Arena arena)
            throws IOException {
        final Path indexTmp = storagePath.resolve(Utils.INDEX_TMP_FILE);
        final Path indexFile = storagePath.resolve(Utils.INDEX_FILE);

        try {
            Files.createFile(indexFile);
        } catch (FileAlreadyExistsException ignored) {
            // it is ok, actually it is normal state
        }
        List<String> existedFiles = Files.readAllLines(indexFile, StandardCharsets.UTF_8);
        String newFileName = Utils.SSTABLE_PREFIX + existedFiles.size();

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
                        storagePath.resolve(newFileName),
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
                    fileSegment.set(ValueLayout.JAVA_LONG_UNALIGNED, indexOffset, Utils.tombstone(dataOffset));
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

        Files.deleteIfExists(indexFile);

        Files.move(indexTmp, indexFile, StandardCopyOption.ATOMIC_MOVE);

        if (arena.scope().isAlive()) {
            newStable(storagePath.resolve(newFileName), arena);
        }
    }

    public void newStable(Path file, Arena arena) {
        try (FileChannel fileChannel = FileChannel.open(file, StandardOpenOption.READ, StandardOpenOption.WRITE)) {
            MemorySegment fileSegment = fileChannel.map(
                    FileChannel.MapMode.READ_WRITE,
                    0,
                    Files.size(file),
                    arena
            );
            segmentList.add(fileSegment);
        } catch (IOException e) {
            throw new IllegalStateException("Error open after flush", e);
        }
    }

    public static void compact(Path storagePath, Iterable<Entry<MemorySegment>> iterable)
            throws IOException {

        List<Path> filesToDelete = collectCompactionTargets(storagePath);
        Path compactionTmpFile = storagePath.resolve("compaction.tmp");

        long[] sizeMetrics = calculateDataAndIndexSize(iterable);
        long dataSize = sizeMetrics[0];
        long indexSize = sizeMetrics[1];

        try (
                FileChannel fileChannel = FileChannel.open(
                        compactionTmpFile,
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

            populateFileSegment(fileSegment, iterable, indexSize);
        }

        Files.move(
                compactionTmpFile,
                storagePath.resolve("compaction"),
                StandardCopyOption.ATOMIC_MOVE,
                StandardCopyOption.REPLACE_EXISTING
        );

        Utils.completeCompaction(storagePath, filesToDelete);
    }

    private static void populateFileSegment(MemorySegment fileSegment,
                                            Iterable<Entry<MemorySegment>> iterable,
                                            long indexSize) {
        long dataOffset = indexSize;
        int indexOffset = 0;
        for (Entry<MemorySegment> entry : iterable) {
            fileSegment.set(ValueLayout.JAVA_LONG_UNALIGNED, indexOffset, dataOffset);
            dataOffset += entry.key().byteSize();
            indexOffset += Long.BYTES;

            MemorySegment value = entry.value();
            if (value == null) {
                fileSegment.set(ValueLayout.JAVA_LONG_UNALIGNED, indexOffset, Utils.tombstone(dataOffset));
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

    private static List<Path> collectCompactionTargets(Path storagePath) throws IOException {
        try (Stream<Path> stream = Files.find(storagePath, 1,
                (path, attributes) -> path.getFileName().toString().startsWith(Utils.SSTABLE_PREFIX))) {
            return stream.toList();
        }
    }

    private static long[] calculateDataAndIndexSize(Iterable<Entry<MemorySegment>> iterable) {
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
        return new long[]{dataSize, count * 2 * Long.BYTES};
    }

    public static List<MemorySegment> loadOrRecover(Path pathToStorage, Arena arena) throws IOException {
        if (Files.exists(Utils.compactionFile(pathToStorage))) {
            Utils.completeCompaction(pathToStorage, Collections.emptyList());
        }

        Path tempIndexFile = pathToStorage.resolve(Utils.INDEX_TMP_FILE);
        Path permanentIndexFile = pathToStorage.resolve(Utils.INDEX_FILE);

        ensureIndexFileExists(tempIndexFile, permanentIndexFile);

        List<String> existedFiles = Files.readAllLines(permanentIndexFile, StandardCharsets.UTF_8);
        List<MemorySegment> segments = new ArrayList<>(existedFiles.size());
        for (String fileName : existedFiles) {
            Path file = pathToStorage.resolve(fileName);
            try (FileChannel fileChannel = FileChannel.open(file, StandardOpenOption.READ, StandardOpenOption.WRITE)) {
                MemorySegment fileSegment = fileChannel.map(
                        FileChannel.MapMode.READ_WRITE,
                        0,
                        Files.size(file),
                        arena
                );
                segments.add(fileSegment);
            }
        }

        return segments;
    }

    private static void ensureIndexFileExists(Path temIndex, Path permanentInd) throws IOException {
        if (!Files.exists(permanentInd)) {
            if (Files.exists(temIndex)) {
                Files.move(temIndex, permanentInd, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
            } else {
                Files.createFile(permanentInd);
            }
        }
    }
}
