package ru.vk.itmo.cheshevandrey;

import ru.vk.itmo.Entry;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.channels.FileChannel;
import java.nio.file.*;
import java.util.*;

public class DiskStorage {

    private static final String STORAGE_META_NAME = "meta.mt";
    private static final String STORAGE_META_TMP_NAME = "meta.tmp";

    static final String INDEX_NAME = "index.idx";
    private static final String INDEX_TMP_NAME = "index.tmp";
    private static final String COMPACTION_NAME = "compaction.cmp";
    private static final String COMPACTION_TMP_NAME = "compaction.tmp";

    public static void compact(IterableDisk iterable,
                               Path storagePath,
                               Path mainDir,
                               Path secondaryDir,
                               int ssTablesCount) throws IOException {
        if (!iterable.iterator().hasNext()) {
            return;
        }

        // Изменили рабочую директорию.
        // После выполнения флаш будет осуществляться в другую директорию.
        changeWorkDir(storagePath, secondaryDir);

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

        // Заменяем файл "0" в новой актуальной директории.
        Files.move(
                storagePath.resolve(COMPACTION_NAME),
                secondaryDir.resolve("0"),
                StandardCopyOption.ATOMIC_MOVE,
                StandardCopyOption.REPLACE_EXISTING
        );

        Path indexPath = mainDir.resolve(INDEX_NAME);

        String zeroFile = "0";
        String oneFile = "1";
        List<String> files = new ArrayList<>(2);
        files.add(zeroFile);
        files.add(oneFile);
        updateIndex(files, indexPath);

        // Удаляем файлы, которые использовались при компакте.
        for (int i = ssTablesCount - 1; i > 1; i--) {
            Files.delete(mainDir.resolve(String.valueOf(i)));
        }

        // Оставляем два пустых.
        resetFile(storagePath, mainDir.resolve("0"));
        resetFile(storagePath, mainDir.resolve("1"));
    }

    private static void resetFile(Path storagePath, Path fileToReset) throws IOException {
        Path tmpFile = storagePath.resolve("tmp.tmp");
        try {
            Files.createFile(tmpFile);
        } catch (FileAlreadyExistsException ignored) {
            // it's ok.
        }
        Files.move(
                tmpFile,
                fileToReset,
                StandardCopyOption.ATOMIC_MOVE,
                StandardCopyOption.REPLACE_EXISTING
        );
    }

    private static void changeWorkDir(Path storagePath, Path newDir) throws IOException {
        Path metaTmpPath = storagePath.resolve(STORAGE_META_TMP_NAME);
        Path metaPath = storagePath.resolve(STORAGE_META_NAME);

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

    public static void completeCompactIfNeeded() throws IOException {
        // -
    }

    public static void save(Iterable<Entry<MemorySegment>> iterable, Path storagePath) throws IOException {
        if (!iterable.iterator().hasNext()) {
            return;
        }

        String dirBeforeSave = readWorkDir(storagePath);
        Path dir = storagePath.resolve(dirBeforeSave);

        Path indexFilePath = dir.resolve(INDEX_NAME);
        List<String> files = Files.readAllLines(indexFilePath);
        String newFileName = String.valueOf(files.size());
        files.add(newFileName);

        Path newFilePath = dir.resolve(newFileName);
        Files.deleteIfExists(newFilePath);
        saveSsTable(newFilePath, iterable);

        // Во время флаша был вызван компакт и изменена основная директория =>
        // => перемещаем в зарезервированный файл "1" новой основной директории.
        String dirAfterSave = readWorkDir(storagePath);
        if (dirBeforeSave.equals(dirAfterSave)) {
            Files.move(
                    newFilePath,
                    storagePath.resolve(dirAfterSave).resolve("1"),
                    StandardCopyOption.ATOMIC_MOVE,
                    StandardCopyOption.REPLACE_EXISTING
            );
        }
        // Иначе оставляем в текущей.
        // Если после этого обновления индекса был начат компакт без этого файла,
        // то он не будет потерян при зачистке, а будет найден и перемещен после компакта.
        else {
            updateIndex(files, indexFilePath);
        }
    }

    static String readWorkDir(Path storagePath) throws IOException {
        Path metaPath = storagePath.resolve(STORAGE_META_NAME);
        if (!Files.exists(metaPath)) {
            return "0";
        }
        return Files.readAllLines(metaPath).get(0);
    }

    static void updateIndex(List<String> files, Path dir) throws IOException {
        Path indexPath = dir.resolve(INDEX_NAME);
        Path indexTmpPath = dir.resolve(INDEX_TMP_NAME);

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
