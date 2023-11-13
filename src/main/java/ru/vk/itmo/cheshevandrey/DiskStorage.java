package ru.vk.itmo.cheshevandrey;

import ru.vk.itmo.BaseEntry;
import ru.vk.itmo.Entry;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.channels.FileChannel;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.*;

public class DiskStorage {

    private final List<MemorySegment> segmentList;

    private static final String INDEX_TMP_NAME = "index.tmp";
    private static final String COMPACTION_INDEX_TMP_NAME = "compaction-index.tmp";
    private static final String COMPACTION_TMP_NAME = "compaction.tmp";
    private static final String SSTABLE_TMP_NAME = "sstable.tmp";
    private static final String COMPACTION_NAME = "compaction.cmp";
    private static final String INDEX_NAME = "index.idx";

    public DiskStorage(List<MemorySegment> segmentList) {
        this.segmentList = segmentList;
    }

    public Iterator<Entry<MemorySegment>> range(
            Iterator<Entry<MemorySegment>> firstIterator,
            Iterator<Entry<MemorySegment>> secondIterator,
            MemorySegment from,
            MemorySegment to) {
        List<Iterator<Entry<MemorySegment>>> iterators = getDiskIterators(from, to);
        iterators.add(secondIterator);
        iterators.add(firstIterator);

        return new MergeIterator<>(iterators, Comparator.comparing(Entry::key, Tools::compare)) {
            @Override
            protected boolean skip(Entry<MemorySegment> memorySegmentEntry) {
                return memorySegmentEntry.value() == null;
            }
        };
    }

    public Iterator<Entry<MemorySegment>> diskRange(
            MemorySegment from,
            MemorySegment to) {
        List<Iterator<Entry<MemorySegment>>> iterators = getDiskIterators(from, to);

        return new MergeIterator<>(iterators, Comparator.comparing(Entry::key, Tools::compare)) {
            @Override
            protected boolean skip(Entry<MemorySegment> memorySegmentEntry) {
                return memorySegmentEntry.value() == null;
            }
        };
    }

    private List<Iterator<Entry<MemorySegment>>> getDiskIterators(MemorySegment from, MemorySegment to) {
        List<Iterator<Entry<MemorySegment>>> iterators = new ArrayList<>(segmentList.size() + 1);
        for (MemorySegment memorySegment : segmentList) {
            iterators.add(iterator(memorySegment, from, to));
        }
        return iterators;
    }

    public void compact(Path storagePath) throws IOException {
        IterableStorage iterable = new IterableStorage(this);
        if (!iterable.iterator().hasNext()) {
            return;
        }

        // Записываем компакшн во временный файл.
        Path compactionTmpPath = storagePath.resolve(COMPACTION_TMP_NAME);
        Files.deleteIfExists(compactionTmpPath);
        saveSsTableToTmpFile(compactionTmpPath, iterable);

        // Записываем во временный индексный файл число файлов, которые используются при компакте.
        try {
            Files.createFile(storagePath.resolve(INDEX_NAME));
        } catch (FileAlreadyExistsException ignored) {
            // it's ok.
        }
        List<String> files = Files.readAllLines(storagePath.resolve(INDEX_NAME));
        Files.write(
                storagePath.resolve(COMPACTION_INDEX_TMP_NAME),
                Collections.singletonList(String.valueOf(files.size())),
                StandardOpenOption.CREATE,
                StandardOpenOption.WRITE
        );

        // Переименовываем временный скомпакченный в актуальный скомпакченный.
        Path compactionPath = storagePath.resolve(COMPACTION_NAME);
        Files.deleteIfExists(compactionPath);
        try {
            Files.createFile(compactionPath);
        } catch (FileAlreadyExistsException ignored) {
            // it's ok.
        }
        Files.move(
                compactionTmpPath,
                compactionPath,
                StandardCopyOption.ATOMIC_MOVE,
                StandardCopyOption.REPLACE_EXISTING
        );

        // При успешном выполнении в данной точке имеем файл COMPACTION_NAME скомпакченных данных
        // и файл COMPACTION_INDEX_TMP_NAME с одним числом (количеством использованных файлов).

        completeCompact(storagePath);
    }

    public static boolean isCompactWasCompletedCorrectly(Path storagePath) {
        return Files.exists(storagePath.resolve(COMPACTION_NAME));
    }

    public static void completeCompact(Path storagePath) throws IOException {
        // Удаляем файлы, которые были использованы при компакте.
        List<String> compactIndexTmpLines = Files.readAllLines(storagePath.resolve(COMPACTION_INDEX_TMP_NAME));
        int compactedSsTablesNumber = Integer.parseInt(compactIndexTmpLines.get(0));
        for (int i = 0; i < compactedSsTablesNumber; i++) {
            Files.delete(storagePath.resolve(String.valueOf(i)));
        }

        // Переименовываем файлы, которые были добавлены флашем на момент компакта
        List<String> files = Files.readAllLines(storagePath.resolve(INDEX_NAME));
        List<String> newFiles = new ArrayList<>();
        newFiles.add("0");
        for (int i = compactedSsTablesNumber; i < files.size(); i++) {
            String newFileName = String.valueOf(i - compactedSsTablesNumber + 1);
            newFiles.add(newFileName);
            Files.move(
                    storagePath.resolve(String.valueOf(i)),
                    storagePath.resolve(newFileName),
                    StandardCopyOption.ATOMIC_MOVE,
                    StandardCopyOption.REPLACE_EXISTING
            );
        }

        // Сохраняем список актуальных файлов.
        Path indexFilePath = storagePath.resolve(INDEX_NAME);
        Files.deleteIfExists(indexFilePath);
        Files.write(
                indexFilePath,
                newFiles,
                StandardOpenOption.CREATE,
                StandardOpenOption.WRITE
        );

        // Переименовываем текущий скомпакченный в актуальный.
        Files.move(
                storagePath.resolve(COMPACTION_NAME),
                storagePath.resolve("0"),
                StandardCopyOption.ATOMIC_MOVE,
                StandardCopyOption.REPLACE_EXISTING
        );
    }

    public static void save(Path storagePath, Iterable<Entry<MemorySegment>> iterable) throws IOException {
        try {
            Files.createFile(storagePath.resolve(INDEX_NAME));
        } catch (FileAlreadyExistsException ignored) {
            // it's ok.
        }
        saveSsTableToTmpFile(storagePath.resolve(SSTABLE_TMP_NAME), iterable);
        moveFilesToActualState(storagePath);
    }

    private static void moveFilesToActualState(Path storagePath) throws IOException {
        List<String> files = Files.readAllLines(storagePath.resolve(INDEX_NAME));
        String newFileName = String.valueOf(files.size());
        files.add(newFileName);

        Path indexTmpPath = storagePath.resolve(INDEX_TMP_NAME);
        try {
            Files.createFile(indexTmpPath);
        } catch (FileAlreadyExistsException ignored) {
            // it's ok.
        }
        Files.write(indexTmpPath, files);

        Files.move(
                storagePath.resolve(INDEX_TMP_NAME),
                storagePath.resolve(INDEX_NAME),
                StandardCopyOption.ATOMIC_MOVE,
                StandardCopyOption.REPLACE_EXISTING
        );

        Path newFilePath = storagePath.resolve(newFileName);
        try {
            Files.createFile(newFilePath);
        } catch (FileAlreadyExistsException ignored) {
            // it's ok.
        }
        Files.move(
                storagePath.resolve(SSTABLE_TMP_NAME),
                newFilePath,
                StandardCopyOption.ATOMIC_MOVE,
                StandardCopyOption.REPLACE_EXISTING
        );
    }

    private static void saveSsTableToTmpFile(Path tmpPath, Iterable<Entry<MemorySegment>> iterable) throws IOException {
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
                        tmpPath,
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
                    fileSegment.set(ValueLayout.JAVA_LONG_UNALIGNED, indexOffset, Tools.tombstone(dataOffset));
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

    public static List<MemorySegment> loadOrRecover(Path storagePath, Arena arena) throws IOException {

        try {
            Files.createFile(storagePath.resolve(INDEX_NAME));
        } catch (FileAlreadyExistsException ignored) {
            // it's ok.
        }
        List<String> files = Files.readAllLines(storagePath.resolve(INDEX_NAME));
        int ssTablesNumber = files.size();

        List<MemorySegment> result = new ArrayList<>(ssTablesNumber);
        for (int ssTable = 0; ssTable < ssTablesNumber; ssTable++) {
            Path file = storagePath.resolve(String.valueOf(ssTable));
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


    private static long indexOf(MemorySegment segment, MemorySegment key) {
        long recordsCount = Tools.recordsCount(segment);

        long left = 0;
        long right = recordsCount - 1;
        while (left <= right) {
            long mid = (left + right) >>> 1;

            long startOfKey = Tools.startOfKey(segment, mid);
            long endOfKey = Tools.endOfKey(segment, mid);
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

        return Tools.tombstone(left);
    }

    private static Iterator<Entry<MemorySegment>> iterator(MemorySegment page, MemorySegment from, MemorySegment to) {
        long recordIndexFrom = from == null ? 0 : Tools.normalize(indexOf(page, from));
        long recordIndexTo = to == null ? Tools.recordsCount(page) : Tools.normalize(indexOf(page, to));
        long recordsCount = Tools.recordsCount(page);

        return new Iterator<>() {
            long index = recordIndexFrom;

            @Override
            public boolean hasNext() {
                return index < recordIndexTo;
            }

            @Override
            public Entry<MemorySegment> next() {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }
                MemorySegment key = Tools.slice(page, Tools.startOfKey(page, index), Tools.endOfKey(page, index));
                long startOfValue = Tools.startOfValue(page, index);
                MemorySegment value =
                        startOfValue < 0
                                ? null
                                : Tools.slice(page, startOfValue, Tools.endOfValue(page, index, recordsCount));
                index++;
                return new BaseEntry<>(key, value);
            }
        };
    }

    private static final class IterableStorage implements Iterable<Entry<MemorySegment>> {
        DiskStorage diskStorage;

        private IterableStorage(DiskStorage diskStorage) {
            this.diskStorage = diskStorage;
        }

        @Override
        public Iterator<Entry<MemorySegment>> iterator() {
            return diskStorage.diskRange(null, null);
        }
    }
}
