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
import java.nio.channels.FileChannel;
import java.nio.file.DirectoryStream;
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
    private final List<Path> ssTables;
    private final List<Path> ssIndexes;
    private final Map<Path, Long> ssTableIndexStorage;
    private final List<FileIterator> fileIterators = new ArrayList<>();

    public FileManager(Config config) {
        basePath = config.basePath();
        ssTables = new ArrayList<>();
        ssIndexes = new ArrayList<>();
        ssTableIndexStorage = new ConcurrentHashMap<>();
        if (Files.exists(basePath)) {
            try (DirectoryStream<Path> directoryStream = Files.newDirectoryStream(basePath)) {
                for (Path path : directoryStream) {
                    String fileName = String.valueOf(path.getFileName());
                    if (fileName.contains(FILE_NAME)) {
                        ssTables.add(path);
                    } else if (fileName.contains(FILE_INDEX_NAME)) {
                        ssIndexes.add(path);
                    }
                }
            } catch (IOException e) {
                throw new IllegalStateException("An error occurred while reading the file.", e);
            }
        }

        ssTables.sort(new PathsComparator(FILE_NAME, FILE_EXTENSION));
        ssIndexes.sort(new PathsComparator(FILE_INDEX_NAME, FILE_EXTENSION));
        filesCount = ssTables.size();
        for (int i = 0; i < filesCount; i++) {
            ssTableIndexStorage.put(ssIndexes.get(i), getIndexSize(ssIndexes.get(i)));
        }
    }

    public void save(NavigableMap<MemorySegment, Entry<MemorySegment>> storage) throws IOException {
        for (FileIterator iterator : fileIterators) {
            if (iterator != null) {
                iterator.close();
            }
        }

        if (storage.isEmpty()) {
            return;
        }

        saveData(storage);
        saveIndexes(storage);
        filesCount++;
    }

    public Iterator<Entry<MemorySegment>> createIterators(MemorySegment from, MemorySegment to) {
        List<PeekingIterator> peekIterators = new ArrayList<>();
        for (int i = 0; i < ssTables.size(); i++) {
            Iterator<Entry<MemorySegment>> iterator = createFileIterator(ssTables.get(i), ssIndexes.get(i), from, to);
            peekIterators.add(new PeekingIterator(i, iterator));
        }

        return MergedIterator.createMergedIterator(peekIterators, new EntryKeyComparator());
    }

    public void clearFileIterators() {
        for (FileIterator iterator : fileIterators) {
            try {
                iterator.close();
            } catch (IOException e) {
                throw new UncheckedIOException("An error occurred while trying to close the iterator.", e);
            }
        }
    }

    private Iterator<Entry<MemorySegment>> createFileIterator(Path ssTable, Path ssIndex,
                                                              MemorySegment from, MemorySegment to) {
        long indexSize = ssTableIndexStorage.get(ssIndex);
        FileIterator fileIterator;
        try {
            fileIterator = new FileIterator(ssTable, ssIndex, from, to, indexSize);
            fileIterators.add(fileIterator);
            return fileIterator;
        } catch (IOException e) {
            throw new UncheckedIOException("An error occurred while reading files.", e);
        }
    }

    private void saveData(NavigableMap<MemorySegment, Entry<MemorySegment>> storage) throws IOException {
        Path filePath = basePath.resolve(FILE_NAME + filesCount + FILE_EXTENSION);
        if (!Files.exists(filePath)) {
            Files.createFile(filePath);
        }

        try (RandomAccessFile randomAccessFile = new RandomAccessFile(String.valueOf(filePath), "rw"))
        {
            for (Map.Entry<MemorySegment, Entry<MemorySegment>> entry : storage.entrySet()) {
                Entry<MemorySegment> entryValue = entry.getValue();
                randomAccessFile.writeInt((int) entryValue.key().byteSize());
                randomAccessFile.write(entryValue.key().toArray(ValueLayout.JAVA_BYTE));

                if (entryValue.value() == null) {
                    randomAccessFile.writeInt(-1);
                } else {
                    randomAccessFile.writeInt((int) entryValue.value().byteSize());
                    randomAccessFile.write(entryValue.value().toArray(ValueLayout.JAVA_BYTE));
                }
            }
        }
    }

    private void saveIndexes(NavigableMap<MemorySegment, Entry<MemorySegment>> storage) throws IOException {
        Path indexPath = basePath.resolve(FILE_INDEX_NAME + filesCount + FILE_EXTENSION);
        if (!Files.exists(indexPath)) {
            Files.createFile(indexPath);
        }

        try (RandomAccessFile randomAccessFile = new RandomAccessFile(String.valueOf(indexPath), "rw"))
        {
            randomAccessFile.writeLong(storage.size());
            long offset = 0;
            for (Map.Entry<MemorySegment, Entry<MemorySegment>> entry : storage.entrySet()) {
                randomAccessFile.writeLong(offset);
                Entry<MemorySegment> entryValue = entry.getValue();
                offset += Integer.BYTES + entryValue.key().byteSize();
                offset += Integer.BYTES;
                if (entryValue.value() != null) {
                    offset += entryValue.value().byteSize();
                }
            }
        }
    }

    private long getIndexSize(Path indexPath) {
        long size;
        try (RandomAccessFile raf = new RandomAccessFile(indexPath.toString(), "r")) {
            size = raf.readLong();
        } catch (IOException e) {
            throw new UncheckedIOException("Unable to read file.", e);
        }

        return size;
    }

    static long getEntryIndex(FileChannel table, FileChannel index,
                              MemorySegment key, long indexSize) throws IOException {
        long low = 0;
        long high = indexSize - 1;
        long mid = (low + high) / 2;
        while (low <= high) {
            Entry<MemorySegment> current = getCurrentEntry(mid, table, index);
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

    static Entry<MemorySegment> getCurrentEntry(long position, FileChannel ssTable,
                                                FileChannel ssIndex) throws IOException {
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment ssTableMemorySegment = ssTable.map(FileChannel.MapMode.READ_ONLY,
                    0, ssTable.size(), arena);
            MemorySegment ssIndexMemorySegment = ssIndex.map(FileChannel.MapMode.READ_ONLY,
                    0, ssIndex.size(), arena);

            long offset = ssIndexMemorySegment.asSlice((position + 1) * Long.BYTES,
                    Long.BYTES).asByteBuffer().getLong();
            int keyLength = ssTableMemorySegment.asSlice(offset, Integer.BYTES).asByteBuffer().getInt();
            offset += Integer.BYTES;
            byte[] keyByteArray = ssTableMemorySegment.asSlice(offset, keyLength).toArray(ValueLayout.JAVA_BYTE);
            offset += keyLength;
            int valueLength = ssTableMemorySegment.asSlice(offset, Integer.BYTES).asByteBuffer().getInt();
            if (valueLength == -1) {
                return new BaseEntry<>(MemorySegment.ofArray(keyByteArray), null);
            }

            offset += Integer.BYTES;
            byte[] valueByteArray = ssTableMemorySegment.asSlice(offset, valueLength).toArray(ValueLayout.JAVA_BYTE);
            return new BaseEntry<>(MemorySegment.ofArray(keyByteArray), MemorySegment.ofArray(valueByteArray));
        }
    }
}
