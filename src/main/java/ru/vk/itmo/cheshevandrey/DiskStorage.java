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
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

public class DiskStorage {

    private final List<MemorySegment> segmentList;

    private static final String INDEX_TMP_FILE_NAME = "index.tmp";
    private static final String INDEX_FILE_NAME = "index.idx";
    private static final String NEW_SSTABLE_FILE_NAME = "newSsTable.tmp";
    private static final String COMPACT_FILE_NAME = "compact.cmpct";

    public DiskStorage(Arena arena, Path storagePath) throws IOException {

        Path indexFile = storagePath.resolve(INDEX_FILE_NAME);
        try {
            Files.createFile(indexFile);
        } catch (FileAlreadyExistsException ignored) {
            // it's ok.
        }

        // Если существует скомпакченный файл, то ранее произошла ошибка в методе bringUpToDateState().
        // Следовательно, необходимо доделать компакт, приведя хранилище в актуальное состояние.
        if (Files.exists(storagePath.resolve(COMPACT_FILE_NAME))) {
            bringUpToDateState(storagePath);
        }

        List<String> fileNames = Files.readAllLines(indexFile);
        int filesCount = fileNames.size();
        this.segmentList = new ArrayList<>(filesCount);
        for (int i = 0; i < filesCount; i++) {
            Path file = storagePath.resolve(String.valueOf(i));
            try (FileChannel fileChannel = FileChannel.open(file, StandardOpenOption.READ, StandardOpenOption.WRITE)) {
                MemorySegment fileSegment = fileChannel.map(
                        FileChannel.MapMode.READ_WRITE,
                        0,
                        Files.size(file),
                        arena
                );
                segmentList.add(fileSegment);
            }
        }
    }

    public Iterator<Entry<MemorySegment>> range(
            Iterator<Entry<MemorySegment>> firstIterator,
            MemorySegment from,
            MemorySegment to) {
        List<Iterator<Entry<MemorySegment>>> iterators = new ArrayList<>(segmentList.size() + 1);
        for (MemorySegment memorySegment : segmentList) {
            iterators.add(iterator(memorySegment, from, to));
        }
        iterators.add(firstIterator);

        return new MergeIterator<>(iterators, Comparator.comparing(Entry::key, PersistentDao::compare)) {
            @Override
            protected boolean skip(Entry<MemorySegment> memorySegmentEntry) {
                return memorySegmentEntry.value() == null;
            }
        };
    }

    public static void compact(
            Path storagePath,
            DiskStorage diskStorage,
            Iterable<Entry<MemorySegment>> iterableMemTable
    ) throws IOException {
        IterableStorage storage = new IterableStorage(iterableMemTable, diskStorage);
        if (diskStorage.segmentList.size() <= 1 && !iterableMemTable.iterator().hasNext()) {
            return;
        }

        // После успешного выполнения ожидаем увидеть скомпакченый файл COMPACT_FILE_NAME.
        save(storagePath, storage, true);

        bringUpToDateState(storagePath);
    }

    public static void save(Path storagePath, Iterable<Entry<MemorySegment>> iterable, boolean isForCompact)
            throws IOException {
        Path newSsTablePath = storagePath.resolve(NEW_SSTABLE_FILE_NAME);

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
                        newSsTablePath,
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

        if (isForCompact) {
            Files.move(
                    newSsTablePath,
                    storagePath.resolve(COMPACT_FILE_NAME),
                    StandardCopyOption.ATOMIC_MOVE,
                    StandardCopyOption.REPLACE_EXISTING
            );
            return;
        }

        Path indexFile = storagePath.resolve(INDEX_FILE_NAME);
        List<String> fileNames = Files.readAllLines(indexFile);
        String newFileName = String.valueOf(fileNames.size());
        fileNames.add(newFileName);

        updateIndex(storagePath, fileNames);

        Path newFilePath = storagePath.resolve(newFileName);
        Files.move(
                newSsTablePath,
                newFilePath,
                StandardCopyOption.ATOMIC_MOVE,
                StandardCopyOption.REPLACE_EXISTING
        );
    }

    private static void bringUpToDateState(Path storagePath) throws IOException {
        List<String> fileNames = Files.readAllLines(storagePath.resolve(INDEX_FILE_NAME));

        for (int i = 1; i < fileNames.size(); i++) {
            Files.delete(storagePath.resolve(String.valueOf(i)));
        }

        String newFileName = "0";

        updateIndex(storagePath, Collections.singletonList(newFileName));

        Files.move(
                storagePath.resolve(COMPACT_FILE_NAME),
                storagePath.resolve(newFileName),
                StandardCopyOption.ATOMIC_MOVE,
                StandardCopyOption.REPLACE_EXISTING
        );
    }

    private static void updateIndex(Path storagePath, List<String> files) throws IOException {
        Path indexTmpPath = storagePath.resolve(INDEX_TMP_FILE_NAME);
        Path indexPath = storagePath.resolve(INDEX_FILE_NAME);

        Files.deleteIfExists(indexTmpPath);
        Files.write(
                indexTmpPath,
                files,
                StandardOpenOption.CREATE,
                StandardOpenOption.TRUNCATE_EXISTING
        );

        Files.move(
                indexTmpPath,
                indexPath,
                StandardCopyOption.ATOMIC_MOVE,
                StandardCopyOption.REPLACE_EXISTING
        );
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
}
