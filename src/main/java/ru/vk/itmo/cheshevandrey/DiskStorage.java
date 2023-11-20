package ru.vk.itmo.cheshevandrey;

import ru.vk.itmo.Entry;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.channels.FileChannel;
import java.nio.file.*;
import java.util.*;

import static ru.vk.itmo.cheshevandrey.Tools.createDir;
import static ru.vk.itmo.cheshevandrey.Tools.createFile;

public class DiskStorage {

    private final List<MemorySegment> primarySegmentList;
    private final List<MemorySegment> secondarySegmentList;

    private final Dir workDir;
    private final Path mainWorkDir;
    private final Path flushWorkDir;
    private final Path storagePath;

    private static final String DIR_1 = "dir-1";
    private static final String DIR_2 = "dir-2";
    private static final String STORAGE_META_NAME = "meta.mt";
    private static final String STORAGE_META_TMP_NAME = "meta.tmp";

    private static final String INDEX_NAME = "index.idx";
    private static final String INDEX_TMP_NAME = "index.tmp";
    private static final String COMPACTION_NAME = "compaction.cmp";
    private static final String COMPACTION_TMP_NAME = "compaction.tmp";

    private boolean isNewStorage = true;

    public DiskStorage(Path storagePath, Arena arena) throws IOException {
        this.storagePath = storagePath;

        this.workDir = getWorkDir(storagePath);
        mainWorkDir = storagePath.resolve(workDir == Dir.DIR_1 ? DIR_1 : DIR_2);
        flushWorkDir = storagePath.resolve(workDir == Dir.DIR_1 ? DIR_2 : DIR_1);

        init();

        if (!Files.exists(storagePath.resolve(STORAGE_META_NAME))) {
            isNewStorage = false;
        }

        this.primarySegmentList = loadOrRecover(mainWorkDir, arena);
        this.secondarySegmentList = loadOrRecover(flushWorkDir, arena);
    }

    private enum Dir {
        DIR_1, DIR_2
    }

    private void init() throws IOException {
        try {
            Files.createDirectory(storagePath);
        } catch (FileAlreadyExistsException ignored) {
            // it's ok.
        }

        createDir(mainWorkDir);
        createDir(flushWorkDir);

        String zeroFile = "0";
        createFile(mainWorkDir.resolve(zeroFile));
        createFile(flushWorkDir.resolve(zeroFile));

        Path mainIndexPath = mainWorkDir.resolve(INDEX_NAME);
        Path flushIndexPath = flushWorkDir.resolve(INDEX_NAME);

        List<String> singletonList = Collections.singletonList(zeroFile);
        if (!Files.exists(mainIndexPath)) {
            updateIndex(singletonList, mainIndexPath);
        }
        if (!Files.exists(flushIndexPath)) {
            updateIndex(singletonList, flushIndexPath);
        }
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
        List<String> files = Files.readAllLines(indexFilePath);
        int ssTablesNumber = files.size();

        List<MemorySegment> result = new ArrayList<>(ssTablesNumber);
        for (int ssTable = 0; ssTable < ssTablesNumber; ssTable++) {
            Path file = path.resolve(String.valueOf(ssTable));

            // Можем иметь несуществующие файлы записанные в индекс.
            // Такая ситуация может произойти только тогда, когда копия скомпакченная
            // уже перемещена в файл "0" другой рабочей директории (данные не теряются).
            if (!Files.exists(file)) {
                continue;
            }

            long fileSize = Files.size(file);
            if (fileSize == 0) { // Попали на пустой файл "0".
                continue;
            }

            try (FileChannel fileChannel = FileChannel.open(file, StandardOpenOption.READ)) {
                MemorySegment fileSegment = fileChannel.map(
                        FileChannel.MapMode.READ_ONLY,
                        0,
                        fileSize,
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
            MemorySegment to,
            Range range) {

        List<Iterator<Entry<MemorySegment>>> iterators = new ArrayList<>();

        switch (range) {
            case PRIMARY:
                addIterators(iterators, primarySegmentList, from, to);
                break;
            case SECONDARY:
                addIterators(iterators, secondarySegmentList, from, to);
                break;
            case ALL:
                addIterators(iterators, primarySegmentList, from, to);
                addIterators(iterators, secondarySegmentList, from, to);
                iterators.add(secondIterator);
                iterators.add(firstIterator);
                break;
        }

        return new MergeIterator<>(iterators, Comparator.comparing(Entry::key, Tools::compare)) {
            @Override
            protected boolean skip(Entry<MemorySegment> memorySegmentEntry) {
                return memorySegmentEntry.value() == null;
            }
        };
    }

    private void addIterators(List<Iterator<Entry<MemorySegment>>> iterators,
                              List<MemorySegment> segmentList,
                              MemorySegment from,
                              MemorySegment to) {
        for (MemorySegment memorySegment : segmentList) {
            iterators.add(SsTableIterator.iterator(memorySegment, from, to));
        }
    }

    public void compact() throws IOException {
        Range range = (isNewStorage || workDir == Dir.DIR_2) ? Range.SECONDARY : Range.PRIMARY;
        IterableStorage iterable = new IterableStorage(this, range);
        if (!iterable.iterator().hasNext()) {
            return;
        }

        // Записываем компакшн во временный файл.
        Path compactionTmpPath = storagePath.resolve(COMPACTION_TMP_NAME);
        Files.deleteIfExists(compactionTmpPath);
        saveSsTable(compactionTmpPath, iterable);

        // Переименовываем временный скомпакченный в актуальный скомпакченный.
        Path compactionPath = storagePath.resolve(COMPACTION_NAME);
        Files.move(
                compactionTmpPath,
                compactionPath,
                StandardCopyOption.ATOMIC_MOVE,
                StandardCopyOption.REPLACE_EXISTING
        );

        completeCompact();
    }

    public void completeCompact() throws IOException {

        // Заменяем файл "0" в директории, в которую происходит флаш.
        Files.move(
                storagePath.resolve(COMPACTION_NAME),
                flushWorkDir.resolve("0"),
                StandardCopyOption.ATOMIC_MOVE,
                StandardCopyOption.REPLACE_EXISTING
        );

        Path indexPath = mainWorkDir.resolve(INDEX_NAME);
        updateIndex(Collections.singletonList("0"), indexPath);

        // Удаляем файлы, которые использовались при компакте.
        // Файл "0" не удаляем, впоследствии заменяем на пустой.
        List<String> files = Files.readAllLines(indexPath);
        for (int i = files.size(); i > 0; i--) {
            Files.delete(mainWorkDir.resolve(String.valueOf(i)));
        }

        // Файл "0" заменяем на пустой.
        Path zeroFileTmp = storagePath.resolve("0.tmp");
        try {
            Files.createFile(zeroFileTmp);
        } catch (FileAlreadyExistsException ignored) {
            // it's ok.
        }
        Files.move(
                zeroFileTmp,
                mainWorkDir.resolve("0"),
                StandardCopyOption.ATOMIC_MOVE,
                StandardCopyOption.REPLACE_EXISTING
        );

        Path metaTmpPath = storagePath.resolve(STORAGE_META_TMP_NAME);
        Path metaPath = storagePath.resolve(STORAGE_META_NAME);

        String newDir = workDir == Dir.DIR_1 ? "1" : "0";
        Files.deleteIfExists(metaTmpPath);
        Files.write(
                metaTmpPath,
                Collections.singletonList(newDir),
                StandardOpenOption.CREATE,
                StandardOpenOption.TRUNCATE_EXISTING
        );

        // Заменяем файл, отображающий актуальную директорию.
        // По сути в этой точке атомарно меняем состояние.
        Files.move(
                metaTmpPath,
                metaPath,
                StandardCopyOption.ATOMIC_MOVE,
                StandardCopyOption.REPLACE_EXISTING
        );
    }

    public void completeCompactIfNeeded() throws IOException {
//        Path compactionFilePath = storagePath.resolve(COMPACTION_NAME);
//        if (Files.exists(compactionFilePath)) {
//            completeCompact();
//        }
    }

    public void save(Iterable<Entry<MemorySegment>> iterable) throws IOException {
        if (!iterable.iterator().hasNext()) {
            return;
        }

        Path indexFilePath = flushWorkDir.resolve(INDEX_NAME);
        List<String> files = Files.readAllLines(indexFilePath);
        String newFileName = String.valueOf(files.size());
        files.add(newFileName);

        Path newFilePath = flushWorkDir.resolve(newFileName);
        Files.deleteIfExists(newFilePath);
        saveSsTable(newFilePath, iterable);
        updateIndex(files, indexFilePath);
    }

    private void saveSsTable(Path pathToSave, Iterable<Entry<MemorySegment>> iterable) throws IOException {
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

    private void updateIndex(List<String> files, Path indexPath) throws IOException {
        Path indexTmpPath = flushWorkDir.resolve(INDEX_TMP_NAME);

        // Запишем актуальный набор sstable во временный индексный файл.
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
                indexPath,
                StandardCopyOption.ATOMIC_MOVE,
                StandardCopyOption.REPLACE_EXISTING
        );
    }
}
