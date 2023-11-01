package ru.vk.itmo.dyagayalexandra;

import ru.vk.itmo.BaseEntry;
import ru.vk.itmo.Config;
import ru.vk.itmo.Entry;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
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
    private final FileChecker fileChecker;
    private final List<FileIterator> fileIterators = new ArrayList<>();
    private final Arena arena;

    public FileManager(Config config) {
        basePath = config.basePath();
        ssTables = new ArrayList<>();
        ssIndexes = new ArrayList<>();
        ssTablesPaths = new ArrayList<>();
        ssIndexesPaths = new ArrayList<>();
        ssTableIndexStorage = new ConcurrentHashMap<>();
        compactManager = new CompactManager(FILE_NAME, FILE_INDEX_NAME, FILE_EXTENSION);
        fileChecker = new FileChecker(FILE_NAME, FILE_INDEX_NAME, FILE_EXTENSION, compactManager);
        arena = Arena.ofShared();
        try {
            Map<Path, Path> allDataPaths = getAllDataPaths(basePath);
            Map<MemorySegment, MemorySegment> allDataSegments = fileChecker.checkFiles(basePath, allDataPaths,
                    getAllFiles(basePath), arena);
            getData(allDataSegments, allDataPaths);
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
        try (FileChannelsHandler writer = new FileChannelsHandler(filePath, indexPath)) {
            writeInitialPosition(writer.getIndexChannel(), storage.size());
            for (var entry : storage.entrySet()) {
                writeEntry(writer.getFileChannel(), entry);
                offset = writeIndexes(writer.getIndexChannel(), offset, entry);
            }
        }
    }

    private List<Path> getAllFiles(Path basePath) throws IOException {
        List<Path> files = new ArrayList<>();
        if (!Files.exists(basePath)) {
            Files.createDirectory(basePath);
        }

        try (DirectoryStream<Path> directoryStream = Files.newDirectoryStream(basePath)) {
            for (Path path : directoryStream) {
                files.add(path);
            }
        }

        return files;
    }

    private Map<Path, Path> getAllDataPaths(Path basePath) throws IOException {
        List<Path> files = getAllFiles(basePath);

        List<Path> ssTablesPaths = new ArrayList<>();
        List<Path> ssIndexesPaths = new ArrayList<>();
        Map<Path, Path> filePathsMap = new ConcurrentHashMap<>();
        for (Path file : files) {
            if (String.valueOf(file.getFileName()).startsWith(FILE_NAME)) {
                ssTablesPaths.add(file);
            }

            if (String.valueOf(file.getFileName()).startsWith(FILE_INDEX_NAME)) {
                ssIndexesPaths.add(file);
            }
        }

        ssTablesPaths.sort(new PathsComparator(FILE_NAME, FILE_EXTENSION));
        ssIndexesPaths.sort(new PathsComparator(FILE_INDEX_NAME, FILE_EXTENSION));

        int size = ssTablesPaths.size();
        for (int i = 0; i < size; i++) {
            filePathsMap.put(ssTablesPaths.get(i), ssIndexesPaths.get(i));
        }

        return filePathsMap;
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
            FileIterator fileIterator = new FileIterator(ssTable, ssIndex, from, to, indexSize);
            fileIterators.add(fileIterator);
            return fileIterator;
        } catch (IOException e) {
            throw new UncheckedIOException("An error occurred while reading files.", e);
        }
    }

    private void getData(Map<MemorySegment, MemorySegment> allDataSegments, Map<Path, Path> allDataPaths) {
        filesCount = allDataSegments.size();
        for (Map.Entry<MemorySegment, MemorySegment> entry : allDataSegments.entrySet()) {
            ssTables.add(entry.getKey());
            ssIndexes.add(entry.getValue());
        }

        for (Map.Entry<Path, Path> entry : allDataPaths.entrySet()) {
            ssTablesPaths.add(entry.getKey());
            ssIndexesPaths.add(entry.getValue());
        }

        for (int i = 0; i < filesCount; i++) {
            ssTableIndexStorage.put(ssIndexes.get(i), getIndexSize(ssIndexesPaths.get(i)));
        }
    }

    public static void writeEntry(FileChannel fileChannel,
                                  Map.Entry<MemorySegment, Entry<MemorySegment>> entry) throws IOException {
        Entry<MemorySegment> entryValue = entry.getValue();
        int keyLength = (int) entryValue.key().byteSize();
        int valueLength;
        if (entryValue.value() == null) {
            valueLength = 0;
        } else {
            valueLength = (int) entryValue.value().byteSize();
        }

        ByteBuffer buffer = ByteBuffer.allocate(Integer.BYTES + keyLength + Integer.BYTES + valueLength);
        buffer.putInt(keyLength);
        buffer.put(entryValue.key().toArray(ValueLayout.JAVA_BYTE));

        if (entryValue.value() == null) {
            buffer.putInt(-1);
        } else {
            buffer.putInt(valueLength);
            buffer.put(entryValue.value().toArray(ValueLayout.JAVA_BYTE));
        }

        buffer.flip();
        while (buffer.hasRemaining()) {
            fileChannel.write(buffer);
        }
    }

    public static long writeIndexes(FileChannel fileChannel, long offset,
                                    Map.Entry<MemorySegment, Entry<MemorySegment>> entry) throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
        Entry<MemorySegment> entryValue = entry.getValue();
        int valueLength = 0;
        if (entryValue.value() != null) {
            valueLength = (int) entryValue.value().byteSize();
        }

        buffer.putLong(offset);
        buffer.flip();
        while (buffer.hasRemaining()) {
            fileChannel.write(buffer);
        }

        buffer.clear();
        int keyLength = (int) entryValue.key().byteSize();
        return offset + Integer.BYTES + keyLength + Integer.BYTES + valueLength;
    }

    private void writeInitialPosition(FileChannel fileChannel, long storageSize) throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
        buffer.putLong(storageSize);
        buffer.flip();
        fileChannel.write(buffer);
        buffer.clear();
    }

    private long getIndexSize(Path indexPath) {
        long size;
        try (FileChannel fileChannel = FileChannel.open(indexPath, StandardOpenOption.READ)) {
            ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
            fileChannel.read(buffer);
            buffer.flip();
            size = buffer.getLong();
        } catch (IOException e) {
            throw new UncheckedIOException("An error occurred while reading the file.", e);
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

    public static Entry<MemorySegment> getCurrentEntry(long position, MemorySegment originalSsTable,
                                                MemorySegment originalSsIndex) throws IOException {
        MemorySegment ssIndex = originalSsIndex.asSlice((position + 1) * Long.BYTES, Long.BYTES);
        ByteBuffer bufferLong = ByteBuffer.allocate(Long.BYTES);
        bufferLong.put(ssIndex.asByteBuffer());
        bufferLong.flip();
        long offset = bufferLong.getLong();

        MemorySegment ssTable = originalSsTable.asSlice(offset);
        ByteBuffer bufferInt = ByteBuffer.allocate(Integer.BYTES);
        bufferInt.put(ssTable.asSlice(0, Integer.BYTES).asByteBuffer());
        bufferInt.flip();
        int keyLength = bufferInt.getInt();
        bufferInt.clear();
        ByteBuffer key = ByteBuffer.allocate(keyLength);
        key.put(ssTable.asSlice(Integer.BYTES, keyLength).asByteBuffer());

        bufferInt.put(ssTable.asSlice(Integer.BYTES + keyLength, Integer.BYTES).asByteBuffer());
        bufferInt.flip();
        int valueLength = bufferInt.getInt();
        if (valueLength == -1) {
            return new BaseEntry<>(MemorySegment.ofArray(key.array()), null);
        }
        ByteBuffer value = ByteBuffer.allocate(valueLength);
        value.put(ssTable.asSlice(Integer.BYTES + keyLength + Integer.BYTES, valueLength).asByteBuffer());
        return new BaseEntry<>(MemorySegment.ofArray(key.array()), MemorySegment.ofArray(value.array()));
    }

    public void closeArena() {
        arena.close();
    }
}
