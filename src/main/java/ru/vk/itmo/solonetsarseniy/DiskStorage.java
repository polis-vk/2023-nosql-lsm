package ru.vk.itmo.solonetsarseniy;

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
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Set;

public class DiskStorage {

    private final List<MemorySegment> segmentList;
    private static StandardOpenOption[] openOptions = new StandardOpenOption[] {
        StandardOpenOption.WRITE,
        StandardOpenOption.CREATE,
        StandardOpenOption.TRUNCATE_EXISTING
    };
    private static StandardOpenOption[] writeReadCreate = new StandardOpenOption[] {
        StandardOpenOption.WRITE,
        StandardOpenOption.READ,
        StandardOpenOption.CREATE
    };
    private static StandardOpenOption[] writeCreateTruncate = new StandardOpenOption[] {
        StandardOpenOption.WRITE,
        StandardOpenOption.CREATE,
        StandardOpenOption.TRUNCATE_EXISTING
    };
    private static StandardCopyOption[] copyOptions = new StandardCopyOption[] {
        StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING
    };
    private static final String INDEX_TMP = "index.tmp";
    public static final String INDEX_FILE = "index.idx";

    public DiskStorage(List<MemorySegment> segmentList) {
        this.segmentList = segmentList;
    }

    public void doCompact(
        Path storagePath,
        Iterable<Entry<MemorySegment>> iterable
    ) throws IOException {
        save(storagePath, iterable, true);
        Path indexTmp = storagePath.resolve(INDEX_TMP);
        Path indexFile = storagePath.resolve(INDEX_FILE);
        String newFileName = generateNewFileName(indexFile);
        Files.move(indexFile, indexTmp, copyOptions);

        List<String> presentFileNames = calcPresentFileNames(newFileName, indexFile);

        File directory = new File(storagePath.toString());
        for (File file : Objects.requireNonNull(directory.listFiles())) {
            Set<String> fileNames = new HashSet<>(presentFileNames);
            if (file.isFile() && !fileNames.contains(file.getName())) {
                Files.delete(file.toPath());
            }
        }
    }

    public Iterator<Entry<MemorySegment>> range(
        Iterator<Entry<MemorySegment>> firstIterator,
        MemorySegment from,
        MemorySegment to
    ) {
        List<Iterator<Entry<MemorySegment>>> iterators = new ArrayList<>(segmentList.size() + 1);
        for (MemorySegment memorySegment : segmentList) {
            iterators.add(DiskStorageUtils.iterator(memorySegment, from, to));
        }
        iterators.add(firstIterator);

        return new MergeIterator<>(iterators, Comparator.comparing(Entry::key, SolonetsDao::compare)) {
            @Override
            protected boolean skip(Entry<MemorySegment> memorySegmentEntry) {
                return memorySegmentEntry.value() == null;
            }
        };
    }

    public static void save(
        Path storagePath,
        Iterable<Entry<MemorySegment>> iterable
    ) throws IOException {
        save(storagePath, iterable, false);
    }

    private static List<String> calcPresentFileNames(
        String newFileName,
        Path indexPath
    ) throws IOException {
        List<String> files = new ArrayList<>();
        files.add(newFileName);
        Files.write(indexPath, files, openOptions);
        files.add(INDEX_FILE);
        return files;
    }

    private static String generateNewFileName(Path indexPath) throws IOException {
        List<String> existedFiles = Files.readAllLines(indexPath, StandardCharsets.UTF_8);
        int compactionFileName = Integer.parseInt(CompactionHelper.compactionFileName(existedFiles));
        if (compactionFileName == 0) {
            return "0";
        } else {
            return String.valueOf(compactionFileName - 1);
        }
    }

    private static void save(
        Path storagePath,
        Iterable<Entry<MemorySegment>> iterable,
        boolean shouldCompact
    ) throws IOException {
        final Path indexTmp = storagePath.resolve(INDEX_TMP);
        final Path indexFile = storagePath.resolve(INDEX_FILE);

        try {
            Files.createFile(indexFile);
        } catch (FileAlreadyExistsException ignored) {
            // it is ok, actually it is normal state
        }
        List<String> existedFiles = Files.readAllLines(indexFile, StandardCharsets.UTF_8);

        String newFileName;

        if (shouldCompact) {
            newFileName = CompactionHelper.compactionFileName(existedFiles);
        } else {
            newFileName = CompactionHelper.nonCompactionFileName(existedFiles);
        }

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
            FileChannel fileChannel = FileChannel.open(storagePath.resolve(newFileName), writeReadCreate);
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
                    fileSegment.set(
                        ValueLayout.JAVA_LONG_UNALIGNED,
                        indexOffset,
                        DiskStorageUtils.tombstone(dataOffset)
                    );
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

        Files.move(indexFile, indexTmp, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);

        List<String> list = new ArrayList<>(existedFiles.size() + 1);
        list.addAll(existedFiles);
        list.add(newFileName);
        Files.write(indexFile, list, writeCreateTruncate);
        Files.delete(indexTmp);
    }

    public static List<MemorySegment> loadOrRecover(Path storagePath, Arena arena) throws IOException {
        Path indexTmp = storagePath.resolve(INDEX_TMP);
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

}
