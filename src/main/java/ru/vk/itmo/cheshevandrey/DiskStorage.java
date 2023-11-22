package ru.vk.itmo.cheshevandrey;

import ru.vk.itmo.Entry;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.channels.FileChannel;
import java.nio.file.*;
import java.util.*;
import java.util.stream.IntStream;

public class DiskStorage {

    static final String INDEX_FILE = "index.idx";

    private static final String STORAGE_META_FILE = "meta.mt";
    private static final String STORAGE_META_TMP_FILE = "meta.tmp";
    private static final String SAVE_TMP_FILE = "save.tmp";
    private static final String INDEX_TMP_FILE = "index.tmp";
    private static final String COMPACTION_FILE = "compaction.cmp";
    private static final String COMPACTION_TMP_FILE = "compaction.tmp";

    public static void compact(IterableDisk iterable,
                               Path storagePath,
                               Path mainDir,
                               Path intermediateDir,
                               Path secondaryDir) throws IOException {
        if (!iterable.iterator().hasNext()) {
            return;
        }

        // Изменили рабочую директорию.
        // Флаш будет осуществляться в другую директорию.
        changeWorkDir(storagePath, secondaryDir);

        int ssTablesCountBeforeCompact = Files.readAllLines(mainDir.resolve(INDEX_FILE)).size();

        // Записываем компакшн во временный файл.
        Path compactionTmpPath = storagePath.resolve(COMPACTION_TMP_FILE);
        Files.deleteIfExists(compactionTmpPath);
        saveSsTable(compactionTmpPath, iterable);

        // Переименовываем временный скомпакченный в актуальный скомпакченный.
        Path compactionPath = storagePath.resolve(COMPACTION_FILE);
        Files.move(
                compactionTmpPath,
                compactionPath,
                StandardCopyOption.ATOMIC_MOVE,
                StandardCopyOption.REPLACE_EXISTING
        );

        // Перемещаем скомпакченный файл в промежуточную директорию.
        Files.move(
                compactionPath,
                intermediateDir.resolve("0"),
                StandardCopyOption.ATOMIC_MOVE,
                StandardCopyOption.REPLACE_EXISTING
        );

        int ssTablesCountAfterCompact = Files.readAllLines(mainDir.resolve(INDEX_FILE)).size();

        updateIntermediateDir(mainDir, intermediateDir, ssTablesCountBeforeCompact, ssTablesCountAfterCompact);

        mainDirRemoveFiles(mainDir, ssTablesCountAfterCompact);
    }

    private static void updateIntermediateDir(Path mainDir, Path intermediateDir, int beforeCount, int afterCount) throws IOException {
        // Если файлы были добавлены во время компакта, то переместим их в промежуточную директорию.
        int to = 1;
        for (int from = beforeCount; from < afterCount; from++) {
            Files.move(
                    mainDir.resolve(String.valueOf(from)),
                    intermediateDir.resolve(String.valueOf(to++)),
                    StandardCopyOption.ATOMIC_MOVE,
                    StandardCopyOption.REPLACE_EXISTING
            );
        }
        int intermFilesCount = Files.readAllLines(intermediateDir.resolve(INDEX_FILE)).size();
        for (int i = intermFilesCount - 1; i >= to; i--) {
            Files.delete(intermediateDir.resolve(String.valueOf(i)));
        }
        List<String> actualIntermFiles = IntStream.range(0, to).mapToObj(String::valueOf).toList();
        updateIndex(actualIntermFiles, intermediateDir);
    }

    private static void mainDirRemoveFiles(Path path, int count) throws IOException {
        // Сначала обнуляем index.
        Path indexTmpPath = path.resolve(INDEX_TMP_FILE);
        Files.createFile(indexTmpPath);
        Files.move(
                indexTmpPath,
                path.resolve(INDEX_FILE),
                StandardCopyOption.ATOMIC_MOVE,
                StandardCopyOption.REPLACE_EXISTING
        );

        // Потом удаляем файлы.
        for (int i = 0; i < count; i++) {
            Files.delete(path.resolve(String.valueOf(i)));
        }
    }

    private static void changeWorkDir(Path storagePath, Path newDir) throws IOException {
        Path metaTmpPath = storagePath.resolve(STORAGE_META_TMP_FILE);
        Path metaPath = storagePath.resolve(STORAGE_META_FILE);

        Files.deleteIfExists(metaTmpPath);
        Files.write(
                metaTmpPath,
                Collections.singletonList(newDir.getFileName().toString()),
                StandardOpenOption.CREATE,
                StandardOpenOption.TRUNCATE_EXISTING
        );

        // Атомарно заменяем файл, отображающий актуальную директорию.
        // Если после этого был начат флаш, то он будет осуществляться уже в другую директорию.
        Files.move(
                metaTmpPath,
                metaPath,
                StandardCopyOption.ATOMIC_MOVE,
                StandardCopyOption.REPLACE_EXISTING
        );
    }

    public static void save(Iterable<Entry<MemorySegment>> iterable, Path storagePath) throws IOException {
        if (!iterable.iterator().hasNext()) {
            return;
        }

        Path tmpFilePath = storagePath.resolve(SAVE_TMP_FILE);
        Files.deleteIfExists(tmpFilePath);
        saveSsTable(tmpFilePath, iterable);

        String dirToSave = readWorkDir(storagePath);
        Path dirToSavePath = storagePath.resolve(dirToSave);
        Path indexFilePath = dirToSavePath.resolve(INDEX_FILE);
        List<String> files = Files.readAllLines(indexFilePath);
        String newFileName = String.valueOf(files.size());
        files.add(newFileName);

        Files.move(
                tmpFilePath,
                dirToSavePath.resolve(newFileName),
                StandardCopyOption.ATOMIC_MOVE,
                StandardCopyOption.REPLACE_EXISTING
        );

        updateIndex(files, dirToSavePath);
    }

    static String readWorkDir(Path storagePath) throws IOException {
        Path metaPath = storagePath.resolve(STORAGE_META_FILE);
        if (!Files.exists(metaPath)) {
            return "0";
        }
        return Files.readAllLines(metaPath).get(0);
    }

    static void updateIndex(List<String> files, Path dir) throws IOException {
        Path indexPath = dir.resolve(INDEX_FILE);
        Path indexTmpPath = dir.resolve(INDEX_TMP_FILE);

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
}
