package ru.vk.itmo.cheshevandrey;

import ru.vk.itmo.BaseEntry;
import ru.vk.itmo.Entry;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.channels.FileChannel;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.*;
import java.util.stream.IntStream;

public class DiskStorage {

    private final List<MemorySegment> mainSegmentList;
    private final List<MemorySegment> flushedSegmentList;

    private final Dir workDir;
    private final Path mainWorkDir;
    private final Path flushWorkDir;
    private final Path storagePath;

    private static final String DIR_1 = "main-1";
    private static final String DIR_2 = "main-2";
    private static final String STORAGE_META_NAME = "meta.mt";
    private static final String STORAGE_META_TMP_NAME = "meta.tmp";

    private static final String INDEX_TMP_NAME = "index.tmp";
    private static final String COMPACTION_INDEX_TMP_NAME = "compaction-index.idx";
    private static final String COMPACTION_TMP_NAME = "compaction.tmp";
    private static final String COMPACTION_NAME = "compaction.cmp";
    private static final String INDEX_NAME = "index.idx";

    private enum Dir {
        DIR_1, DIR_2
    }

    public DiskStorage(Path storagePath, Arena arena) throws IOException {
        this.storagePath = storagePath;

        this.workDir = getWorkDir(storagePath);
        mainWorkDir = storagePath.resolve(workDir == Dir.DIR_1 ? DIR_1 : DIR_2);
        flushWorkDir = storagePath.resolve(workDir == Dir.DIR_1 ? DIR_2 : DIR_1);
        try {
            Files.createDirectory(mainWorkDir);
        } catch (FileAlreadyExistsException ignored) {
            // it's ok.
        }
        try {
            Files.createDirectory(flushWorkDir);
        } catch (FileAlreadyExistsException ignored) {
            // it's ok.
        }

        this.mainSegmentList = loadOrRecover(mainWorkDir, arena);
        this.flushedSegmentList = loadOrRecover(flushWorkDir, arena);
    }

    private Dir getWorkDir(Path storagePath) throws IOException {
        Path metaPath = storagePath.resolve(STORAGE_META_NAME);

        if (!Files.exists(metaPath)) {
            return Dir.DIR_1;
        }

        String firstString = Files.readAllLines(metaPath).get(0);
        int dirIndex = Integer.parseInt(firstString);

        return dirIndex > 0 ? Dir.DIR_2 : Dir.DIR_1;
    }

    private List<MemorySegment> loadOrRecover(Path path, Arena arena) throws IOException {
        Path indexFilePath = path.resolve(INDEX_NAME);
        Tools.createFile(indexFilePath);
        List<String> files = Files.readAllLines(indexFilePath);
        int ssTablesNumber = files.size();

        List<MemorySegment> result = new ArrayList<>(ssTablesNumber);
        for (int ssTable = 0; ssTable < ssTablesNumber; ssTable++) {
            Path file = path.resolve(String.valueOf(ssTable));
            try (FileChannel fileChannel = FileChannel.open(file, StandardOpenOption.READ)) {
                MemorySegment fileSegment = fileChannel.map(
                        FileChannel.MapMode.READ_ONLY,
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
            Iterator<Entry<MemorySegment>> secondIterator,
            MemorySegment from,
            MemorySegment to) {

        int size = mainSegmentList.size() + flushedSegmentList.size() + 2;
        List<Iterator<Entry<MemorySegment>>> iterators = new ArrayList<>(size);
        for (MemorySegment memorySegment : mainSegmentList) {
            iterators.add(iterator(memorySegment, from, to));
        }
        for (MemorySegment memorySegment : flushedSegmentList) {
            iterators.add(iterator(memorySegment, from, to));
        }
        iterators.add(secondIterator);
        iterators.add(firstIterator);

        return new MergeIterator<>(iterators, Comparator.comparing(Entry::key, Tools::compare)) {
            @Override
            protected boolean skip(Entry<MemorySegment> memorySegmentEntry) {
                return memorySegmentEntry.value() == null;
            }
        };
    }

    public void compact() throws IOException {
        IterableStorage iterable = new IterableStorage(this);
        if (!iterable.iterator().hasNext()) {
            return;
        }

        // Записываем названия файлов, которые используются при компакте.
        List<String> compactFiles = IntStream.range(0, mainSegmentList.size()).mapToObj(Integer::toString).toList();
        Files.write(
                mainWorkDir.resolve(COMPACTION_INDEX_TMP_NAME),
                compactFiles,
                StandardOpenOption.CREATE,
                StandardOpenOption.TRUNCATE_EXISTING
        );

        // Записываем компакшн во временный файл.
        Path compactionTmpPath = mainWorkDir.resolve(COMPACTION_TMP_NAME);
        Files.deleteIfExists(compactionTmpPath);
        saveSsTable(compactionTmpPath, iterable);

        // Переименовываем временный скомпакченный в актуальный скомпакченный.
        Path compactionPath = mainWorkDir.resolve(COMPACTION_NAME);
        Files.deleteIfExists(compactionPath);
        Tools.createFile(compactionPath);
        Files.move(
                compactionTmpPath,
                compactionPath,
                StandardCopyOption.ATOMIC_MOVE,
                StandardCopyOption.REPLACE_EXISTING
        );

        // При успешном выполнении в данной точке имеем файл COMPACTION_NAME скомпакченных данных
        // и файл COMPACTION_INDEX_TMP_NAME с одним числом (количеством использованных файлов).

        completeCompact();
    }

    public void completeCompact() throws IOException {

        Path metaTmpPath = storagePath.resolve(STORAGE_META_TMP_NAME);
        Path metaPath = flushWorkDir.resolve(STORAGE_META_NAME);

        Tools.createFile(metaPath);

        String newDir = workDir == Dir.DIR_1 ? "1" : "0";
        Files.deleteIfExists(metaTmpPath);
        Files.write(
                metaTmpPath,
                Collections.singletonList(newDir),
                StandardOpenOption.CREATE,
                StandardOpenOption.TRUNCATE_EXISTING
        );

        // Заменяем файл в директории, в которую происходит флаш.
        Files.move(
                mainWorkDir.resolve(COMPACTION_NAME),
                flushWorkDir.resolve("0"),
                StandardCopyOption.ATOMIC_MOVE,
                StandardCopyOption.REPLACE_EXISTING
        );

        // Заменяем файл, отображающий актуальную директорию.
        Files.move(
                metaTmpPath,
                metaPath,
                StandardCopyOption.ATOMIC_MOVE,
                StandardCopyOption.REPLACE_EXISTING
        );

        // Очищаем предыдущую рабочую директорию.
        deleteFilesInsideDir(mainWorkDir);
    }

    public void completeCompactIfNeeded() throws IOException {
        Path compactionFilePath = mainWorkDir.resolve(COMPACTION_NAME);
        if (Files.exists(compactionFilePath)) {
            completeCompact();
        }
    }

    public void save(Iterable<Entry<MemorySegment>> iterable) throws IOException {
        Path indexFilePath = flushWorkDir.resolve(INDEX_NAME);
        Path indexTmpPath = flushWorkDir.resolve(INDEX_TMP_NAME);

        Tools.createFile(indexFilePath);
        List<String> files = Files.readAllLines(indexFilePath);
        String newFileName = String.valueOf(files.size());
        Path newFilePath = mainWorkDir.resolve(newFileName);

        Files.deleteIfExists(newFilePath);
        saveSsTable(newFilePath, iterable);

        // Запишем актуальный набор sstable во временный индексный файл.
        files.add(newFileName);
        Files.deleteIfExists(indexTmpPath);
        Files.write(
                indexTmpPath,
                files,
                StandardOpenOption.CREATE,
                StandardOpenOption.TRUNCATE_EXISTING
        );

        // Переименуем временный индексный в актуальный.
        Files.move(
                indexTmpPath,
                indexFilePath,
                StandardCopyOption.ATOMIC_MOVE,
                StandardCopyOption.REPLACE_EXISTING
        );
    }

    private static void saveSsTable(Path pathToSave, Iterable<Entry<MemorySegment>> iterable) throws IOException {
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
                        pathToSave,
                        StandardOpenOption.WRITE,
                        StandardOpenOption.READ,
                        StandardOpenOption.CREATE,
                        StandardOpenOption.TRUNCATE_EXISTING
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

    private static void deleteFilesInsideDir(Path dir) throws IOException {
        Files.walkFileTree(dir, new SimpleFileVisitor<>() {
            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                Files.delete(file);
                return FileVisitResult.CONTINUE;
            }
        });
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
            return diskStorage.range(Collections.emptyIterator(), Collections.emptyIterator(), null, null);
        }
    }
}
