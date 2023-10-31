package ru.vk.itmo.tuzikovalexandr;

import ru.vk.itmo.BaseEntry;
import ru.vk.itmo.Config;
import ru.vk.itmo.Entry;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;

import static java.nio.channels.FileChannel.MapMode.READ_ONLY;
import static java.nio.channels.FileChannel.MapMode.READ_WRITE;

public class SSTable {

    private static final Set<OpenOption> openOptions = Set.of(
            StandardOpenOption.CREATE, StandardOpenOption.READ,
            StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING
    );

    private final Path basePath;
    private static final String FILE_PREFIX = "data_";
    private static final String OFFSET_PREFIX = "offset_";
    private static final String INDEX_FILE = "index.idx";
    private static final String INDEX_TMP = "index.tmp";
    private final Comparator<MemorySegment> comparator;
    private List<Entry<MemorySegment>> files;

    public SSTable(Config config, Arena arena) throws IOException {
        this.basePath = config.basePath();

        if (Files.notExists(basePath)) {
            comparator = null;
            return;
        }

        comparator = MemorySegmentComparator::compare;
        this.files = loadData(basePath, arena);
    }

    public Iterator<Entry<MemorySegment>> range(
            Iterator<Entry<MemorySegment>> firstIterator,
            MemorySegment from,
            MemorySegment to) {
        List<Iterator<Entry<MemorySegment>>> iterators = new ArrayList<>(files.size() + 1);
        for (Entry<MemorySegment> entry : files) {
            iterators.add(readDataFromTo(entry.key(), entry.value(), from, to));
        }
        iterators.add(firstIterator);
        return new RangeIterator<>(iterators, Comparator.comparing(Entry::key, MemorySegmentComparator::compare)) {
            @Override
            protected boolean skip(Entry<MemorySegment> memorySegmentEntry) {
                return memorySegmentEntry.value() == null;
            }
        };
    }

    // storage format: offsetFile |keyOffset|valueOffset| dataFile |key|value|
    public static void saveMemData(Path basePath, Iterable<Entry<MemorySegment>> entries) throws IOException {
        Path indexTmp = basePath.resolve(INDEX_TMP);
        Path indexFile = basePath.resolve(INDEX_FILE);

        try {
            Files.createFile(indexFile);
        } catch (FileAlreadyExistsException ignored) {
            // it is ok, actually it is normal state
        }
        List<String> existedFiles = Files.readAllLines(indexFile, StandardCharsets.UTF_8);

        String newFileName = getNewFileIndex(existedFiles);

        int countOffsets = 0;
        long offsetData = 0;
        long memorySize = 0;
        for (Entry<MemorySegment> entry : entries) {
            memorySize += entry.key().byteSize();
            if (entry.value() != null) {
                memorySize += entry.value().byteSize();
            }
            if (entry.value() == null) {
                memorySize += Long.BYTES;
            }
            countOffsets++;
        }

        long[] offsets = new long[countOffsets * 2];

        int index = 0;

        try (FileChannel fcData = FileChannel.open(basePath.resolve(FILE_PREFIX + newFileName), openOptions);
             FileChannel fcOffset = FileChannel.open(basePath.resolve(OFFSET_PREFIX + newFileName), openOptions);
             Arena writeArena = Arena.ofConfined()) {

            MemorySegment writeSegmentData = fcData.map(READ_WRITE, 0, memorySize, writeArena);
            MemorySegment writeSegmentOffset = fcOffset.map(
                    READ_WRITE, 0, (long) offsets.length * Long.BYTES, writeArena
            );

            for (Entry<MemorySegment> entry : entries) {
                MemorySegment key = entry.key();
                offsets[index] = offsetData;
                MemorySegment.copy(key, 0, writeSegmentData, offsetData, entry.key().byteSize());
                offsetData += key.byteSize();

                MemorySegment value = entry.value();
                offsets[index + 1] = offsetData;
                if (value != null) {
                    MemorySegment.copy(value, 0, writeSegmentData, offsetData, entry.value().byteSize());
                    offsetData += value.byteSize();
                }
                if (value == null) {
                    writeSegmentData.set(ValueLayout.JAVA_LONG_UNALIGNED, offsetData, -1L);
                    offsetData += Long.BYTES;
                }

                index += 2;
            }

            MemorySegment.copy(
                    MemorySegment.ofArray(offsets), ValueLayout.JAVA_LONG, 0,
                    writeSegmentOffset, ValueLayout.JAVA_LONG,0, offsets.length
            );
        }

        Files.move(indexFile, indexTmp, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);

        List<String> list = new ArrayList<>(existedFiles.size() + 1);
        list.addAll(existedFiles);
        list.add(newFileName);
        Files.write(
                indexFile,
                list,
                StandardOpenOption.WRITE,
                StandardOpenOption.CREATE,
                StandardOpenOption.TRUNCATE_EXISTING
        );

        Files.delete(indexTmp);
    }

    public Entry<MemorySegment> readData(MemorySegment key) {
        if (files == null || key == null) {
            return null;
        }

        for (int i = files.size() - 1; i >= 0; i--) {
            MemorySegment offsetSegment = files.get(i).key();
            MemorySegment dataSegment = files.get(i).value();

            long offsetResult = binarySearch(key, offsetSegment, dataSegment);

            if (offsetResult >= 0) {
                return Utils.getEntryByKeyOffset(offsetResult, offsetSegment, dataSegment);
            }
        }

        return null;
    }

    public Iterator<Entry<MemorySegment>> readDataFromTo(MemorySegment offsetSegment, MemorySegment dataSegment,
                                             MemorySegment from, MemorySegment to) {
        long start = from == null ? 0 : Math.abs(binarySearch(from, offsetSegment, dataSegment));
        long end = to == null ? offsetSegment.byteSize() - Long.BYTES * 2 :
                Math.abs(binarySearch(to, offsetSegment, dataSegment)) - Long.BYTES * 2;

        return new Iterator<>() {
            long currentOffset = start;

            @Override
            public boolean hasNext() {
                return currentOffset <= end;
            }

            @Override
            public Entry<MemorySegment> next() {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }

                Entry<MemorySegment> currentEntry = Utils.getEntryByKeyOffset(currentOffset, offsetSegment, dataSegment);

                currentOffset += Long.BYTES * 2;
                return currentEntry;
            }
        };
    }

    public static void compactData(Path storagePath, Iterable<Entry<MemorySegment>> iterator) throws IOException {
        saveMemData(storagePath, iterator);

        Path indexTmp = storagePath.resolve(INDEX_TMP);
        Path indexFile = storagePath.resolve(INDEX_FILE);

        List<String> existedFiles = Files.readAllLines(indexFile, StandardCharsets.UTF_8);
        Files.move(indexFile, indexTmp, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);

        String lastFileIndex = existedFiles.getLast();

        deleteOldFiles(storagePath, lastFileIndex);

        Files.writeString(
                indexFile,
                lastFileIndex,
                StandardOpenOption.WRITE,
                StandardOpenOption.CREATE,
                StandardOpenOption.TRUNCATE_EXISTING
        );

        Files.delete(indexTmp);
    }

    public static void deleteOldFiles(Path storagePath, String lastFileIndex) throws IOException {
        String lastFileNameOffset = OFFSET_PREFIX + lastFileIndex;
        String lastFileNameData = FILE_PREFIX + lastFileIndex;

        try (DirectoryStream<Path> fileStream = Files.newDirectoryStream(storagePath)) {
            for (Path path : fileStream) {
                String fileName = path.getFileName().toString();
                if (!fileName.equals(INDEX_FILE) && !fileName.equals(lastFileNameData) &&
                        !fileName.equals(lastFileNameOffset) && !fileName.equals(INDEX_TMP)) {
                    Files.delete(path);
                }
            }
        }
    }

    private long binarySearch(MemorySegment key, MemorySegment offsetSegment, MemorySegment dataSegment) {
        long left = 0;
        long right = offsetSegment.byteSize() / Long.BYTES - 1;

        while (left <= right) {

            long middle = (right - left) / 2 + left;

            long offset = middle * Long.BYTES * 2;
            if (offset >= offsetSegment.byteSize()) {
                return -left * Long.BYTES * 2;
            }

            long keyOffset = offsetSegment.get(ValueLayout.JAVA_LONG, offset);

            offset = middle * Long.BYTES * 2 + Long.BYTES;
            long keySize = offsetSegment.get(ValueLayout.JAVA_LONG, offset) - keyOffset;

            MemorySegment keySegment = dataSegment.asSlice(keyOffset, keySize);

            int result = comparator.compare(keySegment, key);

            if (result < 0) {
                left = middle + 1;
            } else if (result > 0) {
                right = middle - 1;
            } else {
                return middle * Long.BYTES * 2;
            }
        }

        return -left * Long.BYTES * 2;
    }

    public static List<Entry<MemorySegment>> loadData(Path storagePath, Arena arena) throws IOException {
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
        List<Entry<MemorySegment>> result = new ArrayList<>(existedFiles.size());
        for (String fileName : existedFiles) {
            Path offsetFullPath = storagePath.resolve(OFFSET_PREFIX + fileName);
            Path fileFullPath = storagePath.resolve(FILE_PREFIX + fileName);

            try (FileChannel fcOffset = FileChannel.open(offsetFullPath, StandardOpenOption.READ);
                 FileChannel fcData = FileChannel.open(fileFullPath, StandardOpenOption.READ)) {
                MemorySegment readSegmentOffset = fcOffset.map(
                        READ_ONLY, 0, Files.size(offsetFullPath), arena
                );
                MemorySegment readSegmentData = fcData.map(
                        READ_ONLY, 0, Files.size(fileFullPath), arena
                );

                result.add(new BaseEntry<>(readSegmentOffset, readSegmentData));
            }
        }

        return result;
    }

    private static String getNewFileIndex(List<String> existedFiles) {
        if (existedFiles.isEmpty()) {
            return "1";
        }

        int lastIndex = Integer.parseInt(existedFiles.getLast());
        return String.valueOf(lastIndex + 1);
    }
}
