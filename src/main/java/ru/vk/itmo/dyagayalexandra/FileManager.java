package ru.vk.itmo.dyagayalexandra;

import ru.vk.itmo.BaseEntry;
import ru.vk.itmo.Config;
import ru.vk.itmo.Entry;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.UncheckedIOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentHashMap;

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
    private final Map<MemorySegment, Long> ssTableIndexStorage;
    private final CompactManager compactManager;
    private final Arena arena;

    public FileManager(Config config) {
        basePath = config.basePath();
        ssTables = new ArrayList<>();
        ssIndexes = new ArrayList<>();
        ssTablesPaths = new ArrayList<>();
        ssIndexesPaths = new ArrayList<>();
        ssTableIndexStorage = new ConcurrentHashMap<>();
        compactManager = new CompactManager(FILE_NAME, FILE_INDEX_NAME, FILE_EXTENSION);
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

        Path filePath = basePath.resolve(FILE_NAME + filesCount + FILE_EXTENSION);
        if (!Files.exists(filePath)) {
            Files.createFile(filePath);
        }

        Path indexPath = basePath.resolve(FILE_INDEX_NAME + filesCount + FILE_EXTENSION);
        if (!Files.exists(indexPath)) {
            Files.createFile(indexPath);
        }

        long offset = 0;
        try (RandomAccessFile randomAccessDataFile = new RandomAccessFile(String.valueOf(filePath), "rw");
             RandomAccessFile randomAccessIndexFile = new RandomAccessFile(String.valueOf(indexPath), "rw")) {
            writeStorageSize(randomAccessIndexFile, storage.size());
            for (var entry : storage.entrySet()) {
                writeEntry(randomAccessDataFile, entry);
                offset = writeIndexes(randomAccessIndexFile, offset, entry);
            }
        }
    }

    public void performCompact(Iterator<Entry<MemorySegment>> iterator, boolean hasDataStorage) throws IOException {
        if (filesCount <= 1 && !hasDataStorage) {
            return;
        }

        compactManager.compact(basePath, iterator);
        compactManager.deleteAllFiles(ssTablesPaths, ssIndexesPaths);
        compactManager.renameCompactFile(basePath);

        ssTables.clear();
        ssIndexes.clear();
        ssTablesPaths.clear();
        ssIndexesPaths.clear();
        ssTableIndexStorage.clear();
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

    public Iterator<Entry<MemorySegment>> createIterators(MemorySegment from, MemorySegment to) {
        List<PeekingIterator> peekIterators = new ArrayList<>();
        for (int i = 0; i < ssTables.size(); i++) {
            Iterator<Entry<MemorySegment>> iterator = createFileIterator(ssTables.get(i), ssIndexes.get(i), from, to);
            peekIterators.add(new PeekingIterator(i, iterator));
        }

        return MergedIterator.createMergedIterator(peekIterators, new EntryKeyComparator());
    }

    private Iterator<Entry<MemorySegment>> createFileIterator(MemorySegment ssTable, MemorySegment ssIndex,
                                                              MemorySegment from, MemorySegment to) {
        try {
            long indexSize = ssTableIndexStorage.get(ssIndex);
            return new FileIterator(ssTable, ssIndex, from, to, indexSize);
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

        for (int i = 0; i < filesCount; i++) {
            ssTableIndexStorage.put(ssIndexes.get(i), getIndexSize(ssIndexesPaths.get(i)));
        }
    }

    static void writeEntry(RandomAccessFile randomAccessDataFile,
                           Map.Entry<MemorySegment, Entry<MemorySegment>> entry) throws IOException {
        Entry<MemorySegment> entryValue = entry.getValue();
        randomAccessDataFile.writeInt((int) entryValue.key().byteSize());
        randomAccessDataFile.write(entryValue.key().toArray(ValueLayout.JAVA_BYTE));

        if (entryValue.value() == null) {
            randomAccessDataFile.writeInt(-1);
        } else {
            randomAccessDataFile.writeInt((int) entryValue.value().byteSize());
            randomAccessDataFile.write(entryValue.value().toArray(ValueLayout.JAVA_BYTE));
        }
    }

    public static long writeIndexes(RandomAccessFile randomAccessIndexFile,
                                    long offset,
                                    Map.Entry<MemorySegment, Entry<MemorySegment>> entry) throws IOException {
        randomAccessIndexFile.writeLong(offset);
        Entry<MemorySegment> entryValue = entry.getValue();
        offset += Integer.BYTES + entryValue.key().byteSize();
        offset += Integer.BYTES;
        if (entryValue.value() != null) {
            offset += entryValue.value().byteSize();
        }

        return offset;
    }


    private void writeStorageSize(RandomAccessFile randomAccessIndexFile, long storageSize) throws IOException {
        randomAccessIndexFile.seek(0);
        randomAccessIndexFile.writeLong(storageSize);
    }

    private long getIndexSize(Path indexPath) {
        long size;
        try (RandomAccessFile randomAccessFile = new RandomAccessFile(indexPath.toString(), "r")) {
            size = randomAccessFile.readLong();
        } catch (IOException e) {
            throw new UncheckedIOException("Unable to read file.", e);
        }

        return size;
    }

    public static long getEntryIndex(MemorySegment ssTable, MemorySegment ssIndex,
                                     MemorySegment key, long indexSize) throws IOException {
        long low = 0;
        long high = indexSize - 1;
        long mid = (low + high) / 2;
        while (low <= high) {
            Entry<MemorySegment> current = getCurrentEntry(mid, ssTable, ssIndex);
            int compare = new MemorySegmentComparator().compare(key, current.key());
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

    public static Entry<MemorySegment> getCurrentEntry(long position, MemorySegment ssTable,
                                                       MemorySegment ssIndex) throws IOException {
        long offset = ssIndex.asSlice((position + 1) * Long.BYTES,
                Long.BYTES).asByteBuffer().getLong();
        int keyLength = ssTable.asSlice(offset, Integer.BYTES).asByteBuffer().getInt();
        offset += Integer.BYTES;
        byte[] keyByteArray = ssTable.asSlice(offset, keyLength).toArray(ValueLayout.JAVA_BYTE);
        offset += keyLength;
        int valueLength = ssTable.asSlice(offset, Integer.BYTES).asByteBuffer().getInt();
        if (valueLength == -1) {
            return new BaseEntry<>(MemorySegment.ofArray(keyByteArray), null);
        }

        offset += Integer.BYTES;
        byte[] valueByteArray = ssTable.asSlice(offset, valueLength).toArray(ValueLayout.JAVA_BYTE);
        return new BaseEntry<>(MemorySegment.ofArray(keyByteArray), MemorySegment.ofArray(valueByteArray));
    }

    public void closeArena() {
        arena.close();
    }
}
