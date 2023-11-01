package ru.vk.itmo.osokindmitry;

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
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

public class DiskStorage {

    private final List<SsTable> tableList;
    private static final String INDEX_FILE_NAME = "index";
    private static final String SSTABLE_EXT = ".sstable";
    private static final String TMP_EXT = ".tmp";

    public DiskStorage(List<Path> ssTablePaths, Arena arena) throws IOException {
        tableList = new ArrayList<>();
        for (Path path : ssTablePaths) {
            tableList.add(new SsTable(path, arena));
        }
    }

    public Iterator<Entry<MemorySegment>> range(
            Iterator<Entry<MemorySegment>> firstIterator,
            MemorySegment from,
            MemorySegment to) {

        List<Iterator<Entry<MemorySegment>>> iterators = new ArrayList<>(tableList.size() + 1);
        for (SsTable ssTable : tableList) {
            iterators.add(ssTable.iterator(from, to));
        }
        iterators.add(firstIterator);

        return new MergeIterator<>(iterators, Comparator.comparing(Entry::key, PersistentDao::compare)) {
            @Override
            protected boolean skip(Entry<MemorySegment> memorySegmentEntry) {
                return memorySegmentEntry.value() == null;
            }
        };
    }

    /**
     * Stores memTable as follows:
     * index:
     * |key0_Start|value0_Start|key1_Start|value1_Start|key2_Start|value2_Start|...
     * key0_Start = data start = end of index
     * data:
     * |key0|value0|key1|value1|...
     */
    public static void save(Path storagePath, Iterable<Entry<MemorySegment>> iterable)
            throws IOException {
        final Path indexTmp = storagePath.resolve(INDEX_FILE_NAME + TMP_EXT);
        final Path indexFile = storagePath.resolve(INDEX_FILE_NAME + SSTABLE_EXT);

        try {
            Files.createFile(indexFile);
        } catch (FileAlreadyExistsException ignored) {
            // it is ok, actually it is normal state
        }
        List<String> existedFiles = Files.readAllLines(indexFile, StandardCharsets.UTF_8);

        String newFileName = existedFiles.size() + SSTABLE_EXT;

        long dataSize = 0;
        long count = 0;

        for (Entry<MemorySegment> entry : iterable) {
            dataSize += entry.key().byteSize();

            if (entry.value() != null) {
                dataSize += entry.value().byteSize();
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

            long dataOffset = indexSize;
            int indexOffset = 0;
            for (Entry<MemorySegment> entry : iterable) {
                fileSegment.set(ValueLayout.JAVA_LONG_UNALIGNED, indexOffset, dataOffset);
                MemorySegment.copy(entry.key(), 0, fileSegment, dataOffset, entry.key().byteSize());
                dataOffset += entry.key().byteSize();
                indexOffset += Long.BYTES;

                if (entry.value() == null) {
                    fileSegment.set(ValueLayout.JAVA_LONG_UNALIGNED, indexOffset, SsTable.tombstone(dataOffset));
                } else {
                    fileSegment.set(ValueLayout.JAVA_LONG_UNALIGNED, indexOffset, dataOffset);
                    MemorySegment.copy(entry.value(), 0, fileSegment, dataOffset, entry.value().byteSize());
                    dataOffset += entry.value().byteSize();
                }
                indexOffset += Long.BYTES;
            }
        }

        updateIndex(indexFile, indexTmp, existedFiles, newFileName);
    }

    private static void updateIndex(Path indexFile, Path indexTmp, List<String> existedFiles, String newFileName)
            throws IOException {
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

    public void compact(Path storagePath) throws IOException {
        if (tableList.isEmpty()) {
            return;
        }
        final Path indexFile = storagePath.resolve(INDEX_FILE_NAME + SSTABLE_EXT);

        try {
            Files.createFile(indexFile);
        } catch (FileAlreadyExistsException ignored) {
            // it is ok, actually it is normal state
        }

        MergeIterator<Entry<MemorySegment>> mergeIterator = getMergeIterator();
        long dataSize = 0;
        long count = 0;
        while (mergeIterator.hasNext()) {
            count++;
            Entry<MemorySegment> next = mergeIterator.next();
            dataSize += next.key().byteSize() + next.value().byteSize();
        }
        dataSize += count * Long.BYTES * 2;

        MemorySegment fileSegment;
        try (
                FileChannel fileChannel = FileChannel.open(
                        storagePath.resolve("0" + TMP_EXT),
                        StandardOpenOption.WRITE,
                        StandardOpenOption.READ,
                        StandardOpenOption.CREATE
                );
                Arena writeArena = Arena.ofConfined()
        ) {
            fileSegment = fileChannel.map(
                    FileChannel.MapMode.READ_WRITE,
                    0,
                    dataSize,
                    writeArena
            );

            mergeIterator = getMergeIterator();
            long dataOffset = count * 2 * Long.BYTES;
            int indexOffset = 0;
            while (mergeIterator.hasNext()) {
                Entry<MemorySegment> entry = mergeIterator.next();

                fileSegment.set(ValueLayout.JAVA_LONG_UNALIGNED, indexOffset, dataOffset);
                MemorySegment.copy(entry.key(), 0, fileSegment, dataOffset, entry.key().byteSize());
                dataOffset += entry.key().byteSize();
                indexOffset += Long.BYTES;

                fileSegment.set(ValueLayout.JAVA_LONG_UNALIGNED, indexOffset, dataOffset);
                MemorySegment.copy(entry.value(), 0, fileSegment, dataOffset, entry.value().byteSize());
                dataOffset += entry.value().byteSize();
                indexOffset += Long.BYTES;
            }
        }

        updateIndexAndCleanUp(storagePath, indexFile);
        tableList.clear();
        tableList.add(new SsTable(fileSegment));
    }

    private void updateIndexAndCleanUp(Path storagePath, Path indexFile) throws IOException {

        final List<String> existedFiles = Files.readAllLines(indexFile, StandardCharsets.UTF_8);
        final Path indexTmp = storagePath.resolve(INDEX_FILE_NAME + TMP_EXT);

        Files.move(indexFile, indexTmp, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
        Files.move(storagePath.resolve("0" + TMP_EXT), storagePath.resolve("0" + SSTABLE_EXT),
                StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);

        Files.writeString(
                indexFile,
                "0" + SSTABLE_EXT,
                StandardOpenOption.WRITE,
                StandardOpenOption.CREATE,
                StandardOpenOption.TRUNCATE_EXISTING
        );

        try {
            for (int i = 1; i < existedFiles.size(); i++) {
                Files.delete(storagePath.resolve(existedFiles.get(i)));
            }
        } catch (IOException e) {
            // If we fail during delete, db will recover with index.tmp that points to deleted files
            Files.delete(indexTmp);
            throw e;
        }

        Files.delete(indexTmp);
    }

    private MergeIterator<Entry<MemorySegment>> getMergeIterator() {
        List<Iterator<Entry<MemorySegment>>> iterators = new ArrayList<>(tableList.size() + 1);
        for (SsTable ssTable : tableList) {
            iterators.add(ssTable.iterator(null, null));
        }

        return new MergeIterator<>(iterators, Comparator.comparing(Entry::key, PersistentDao::compare)) {
            @Override
            protected boolean skip(Entry<MemorySegment> memorySegmentEntry) {
                return memorySegmentEntry.value() == null;
            }
        };
    }

}
