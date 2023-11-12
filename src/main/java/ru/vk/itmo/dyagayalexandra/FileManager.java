package ru.vk.itmo.dyagayalexandra;

import ru.vk.itmo.BaseEntry;
import ru.vk.itmo.Config;
import ru.vk.itmo.Entry;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;

public class FileManager {

    private long filesCount;
    private static final String FILE_NAME = "data";
    private static final String FILE_INDEX_NAME = "index";
    private static final String FILE_EXTENSION = ".txt";
    private final Path basePath;
    private final List<MemorySegment> ssTables;
    private final List<MemorySegment> ssIndexes;
    private final List<Path> ssTablesPaths;
    private final List<Path> ssIndexesPaths;
    private final List<FileIterator> fileIterators;
    private final CompactManager compactManager;
    private final Arena arena;

    public FileManager(Config config) {
        basePath = config.basePath();
        ssTables = new ArrayList<>();
        ssIndexes = new ArrayList<>();
        ssTablesPaths = new ArrayList<>();
        ssIndexesPaths = new ArrayList<>();
        fileIterators = new ArrayList<>();
        compactManager = new CompactManager(FILE_NAME, FILE_INDEX_NAME, FILE_EXTENSION, this);
        FileChecker fileChecker = new FileChecker(FILE_NAME, FILE_INDEX_NAME, FILE_EXTENSION, compactManager);
        arena = Arena.ofShared();
        try {
            List<Map.Entry<MemorySegment, MemorySegment>> allDataSegments = fileChecker.checkFiles(basePath, arena);
            getData(allDataSegments, fileChecker.getAllDataPaths(basePath));
        } catch (IOException e) {
            throw new UncheckedIOException("Error checking files.", e);
        }
    }

    public void save(NavigableMap<MemorySegment, Entry<MemorySegment>> storage) throws IOException {
        if (storage.isEmpty()) {
            return;
        }

        Path tablePath = basePath.resolve(FILE_NAME + filesCount + FILE_EXTENSION);
        Path indexPath = basePath.resolve(FILE_INDEX_NAME + filesCount + FILE_EXTENSION);

        long indexOffset = Long.BYTES;
        long tableSize = 0;
        long[] offsets = new long[storage.entrySet().size()];
        try (FileChannel tableChannel = FileChannel.open(tablePath, StandardOpenOption.READ,
                StandardOpenOption.WRITE, StandardOpenOption.CREATE);
             FileChannel indexChannel = FileChannel.open(indexPath, StandardOpenOption.READ,
                     StandardOpenOption.WRITE, StandardOpenOption.CREATE)) {
            int index = 0;
            for (Map.Entry<MemorySegment, Entry<MemorySegment>> entry : storage.entrySet()) {
                long offset = tableSize;
                tableSize += 2 * Integer.BYTES + entry.getValue().key().byteSize();
                if (entry.getValue().value() != null) {
                    tableSize += entry.getValue().value().byteSize();
                }
                offsets[index++] = tableSize - offset;
            }

            MemorySegment tableMemorySegment = tableChannel.map(FileChannel.MapMode.READ_WRITE,
                    0, tableSize, arena);
            MemorySegment indexMemorySegment = indexChannel.map(FileChannel.MapMode.READ_WRITE,
                    0, (long) (storage.size() + 1) * Long.BYTES, arena);
            writeStorageSize(indexMemorySegment, storage.size());
            index = 0;
            for (Map.Entry<MemorySegment, Entry<MemorySegment>> entry : storage.entrySet()) {
                writeEntry(tableMemorySegment, offsets[index], entry);
                writeIndexes(indexMemorySegment, indexOffset, offsets[index++]);
                indexOffset += Long.BYTES;
            }
        }
    }

    public Entry<MemorySegment> get(MemorySegment key) {
        for (int i = 0; i < ssTables.size(); i++) {
            FileIterator fileIterator;
            try {
                fileIterator = new FileIterator(ssTables.get(i), ssIndexes.get(i), key, null,
                        getIndexSize(ssIndexes.get(i)), this);
            } catch (IOException e) {
                throw new UncheckedIOException("Failed to create FileIterator", e);
            }

            if (fileIterator.hasNext()) {
                Entry<MemorySegment> currentEntry = fileIterator.next();
                if (currentEntry != null && MemorySegmentComparator.INSTANCE.compare(currentEntry.key(), key) == 0) {
                    return currentEntry;
                }
            }
        }

        return null;
    }

    public void performCompact(List<Iterator<Entry<MemorySegment>>> iterators, boolean hasDataStorage) {

        Iterator<Entry<MemorySegment>> iterator;
        iterator = MergedIterator.createMergedIterator(iterators, EntryKeyComparator.INSTANCE);
        if (!iterator.hasNext()) {
            return;
        }

        int count = 1;
        while (iterator.hasNext()) {
            iterator.next();
            count++;
        }

        if (filesCount <= 1 && !hasDataStorage) {
            return;
        }

        try {
            compactManager.compact(basePath, iterators, count);
            compactManager.deleteAllFiles(ssTablesPaths, ssIndexesPaths);
            compactManager.renameCompactFile(basePath);
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to complete compact.", e);
        }

        ssTables.clear();
        ssIndexes.clear();
        ssTablesPaths.clear();
        ssIndexesPaths.clear();
        filesCount = 1;
    }

    void flush(NavigableMap<MemorySegment, Entry<MemorySegment>> storage) {
        try {
            save(storage);
            filesCount++;
        } catch (IOException e) {
            throw new UncheckedIOException("Error saving storage.", e);
        }
    }

    public List<Iterator<Entry<MemorySegment>>> createIterators(MemorySegment from, MemorySegment to) {
        List<Iterator<Entry<MemorySegment>>> iterators = new ArrayList<>();
        for (int i = 0; i < ssTables.size(); i++) {
            iterators.add(createFileIterator(ssTables.get(i), ssIndexes.get(i), from, to));
        }

        return iterators;
    }

    private Iterator<Entry<MemorySegment>> createFileIterator(MemorySegment ssTable, MemorySegment ssIndex,
                                                              MemorySegment from, MemorySegment to) {
        try {
            long indexSize = getIndexSize(ssIndex);
            FileIterator fileIterator = new FileIterator(ssTable, ssIndex, from, to, indexSize, this);
            fileIterators.add(fileIterator);
            return fileIterator;
        } catch (IOException e) {
            throw new UncheckedIOException("An error occurred while reading files.", e);
        }
    }

    private void getData(List<Map.Entry<MemorySegment, MemorySegment>> allDataSegments,
                         List<Map.Entry<Path, Path>> allDataPaths) {
        filesCount = allDataSegments.size();
        for (Map.Entry<MemorySegment, MemorySegment> entry : allDataSegments) {
            ssTables.add(entry.getKey());
            ssIndexes.add(entry.getValue());
        }

        for (Map.Entry<Path, Path> entry : allDataPaths) {
            ssTablesPaths.add(entry.getKey());
            ssIndexesPaths.add(entry.getValue());
        }
    }

    public long writeEntry(MemorySegment tableMemorySegment, long offset,
                           Map.Entry<MemorySegment, Entry<MemorySegment>> entry) {
        Entry<MemorySegment> entryValue = entry.getValue();
        long tableFileOffset = offset;
        tableMemorySegment.set(ValueLayout.JAVA_INT_UNALIGNED, tableFileOffset,
                (int) entryValue.key().byteSize());
        tableFileOffset += Integer.BYTES;

        byte[] keyByteArray = entryValue.key().toArray(ValueLayout.JAVA_BYTE);
        for (byte keyByte : keyByteArray) {
            tableMemorySegment.set(ValueLayout.JAVA_BYTE, tableFileOffset, keyByte);
            tableFileOffset++;
        }

        if (entryValue.value() != null) {
            tableMemorySegment.set(ValueLayout.JAVA_INT_UNALIGNED, tableFileOffset,
                    (int) entryValue.value().byteSize());
            tableFileOffset += Integer.BYTES;
            byte[] valueByteArray = entryValue.value().toArray(ValueLayout.JAVA_BYTE);
            for (byte valueByte : valueByteArray) {
                tableMemorySegment.set(ValueLayout.JAVA_BYTE, tableFileOffset, valueByte);
                tableFileOffset++;
            }
        } else {
            tableMemorySegment.set(ValueLayout.JAVA_INT_UNALIGNED, tableFileOffset, -1);
            tableFileOffset += Integer.BYTES;
        }

        return tableFileOffset;
    }

    public void writeIndexes(MemorySegment indexMemorySegment, long indexOffset, long offset) {
        indexMemorySegment.set(ValueLayout.JAVA_LONG_UNALIGNED, indexOffset, offset);
    }

    private void writeStorageSize(MemorySegment indexMemorySegment, long storageSize) {
        indexMemorySegment.set(ValueLayout.JAVA_LONG_UNALIGNED, 0, storageSize);
    }

    private long getIndexSize(MemorySegment indexMemorySegment) {
        return indexMemorySegment.get(ValueLayout.JAVA_LONG_UNALIGNED, 0);
    }

    public long getEntryIndex(MemorySegment ssTable, MemorySegment ssIndex,
                              MemorySegment key, long indexSize) throws IOException {
        long low = 0;
        long high = indexSize - 1;
        long mid = (low + high) / 2;
        while (low <= high) {
            Entry<MemorySegment> current = getCurrentEntry(mid, ssTable, ssIndex);
            int compare = MemorySegmentComparator.INSTANCE.compare(key, current.key());
            if (compare > 0) {
                low = mid + 1;
            } else if (compare < 0) {
                high = mid - 1;
            } else {
                return mid;
            }
            mid = (low + high) / 2;
        }

        return low;
    }

    public Entry<MemorySegment> getCurrentEntry(long position, MemorySegment ssTable,
                                                MemorySegment ssIndex) throws IOException {

        long offset = ssIndex.get(ValueLayout.JAVA_LONG_UNALIGNED,
                (position + 1) * Long.BYTES);
        int keyLength = ssTable.get(ValueLayout.JAVA_INT_UNALIGNED, offset);
        offset += Integer.BYTES;

        byte[] keyByteArray = new byte[keyLength];
        for (int i = 0; i < keyLength; i++) {
            keyByteArray[i] = ssTable.get(ValueLayout.JAVA_BYTE, offset);
            offset++;
        }

        int valueLength = ssTable.get(ValueLayout.JAVA_INT_UNALIGNED, offset);
        offset += Integer.BYTES;
        if (valueLength == -1) {
            return new BaseEntry<>(MemorySegment.ofArray(keyByteArray), null);
        }

        byte[] valueByteArray = new byte[valueLength];
        for (int i = 0; i < valueLength; i++) {
            valueByteArray[i] = ssTable.get(ValueLayout.JAVA_BYTE, offset);
            offset++;
        }

        return new BaseEntry<>(MemorySegment.ofArray(keyByteArray),
                MemorySegment.ofArray(valueByteArray));
    }

    public void closeArena() {
        arena.close();
    }
}
