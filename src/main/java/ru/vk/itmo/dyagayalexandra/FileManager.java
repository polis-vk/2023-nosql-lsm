package ru.vk.itmo.dyagayalexandra;

import ru.vk.itmo.BaseEntry;
import ru.vk.itmo.Config;
import ru.vk.itmo.Entry;

import java.io.IOException;
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
                for (var path : directoryStream) {
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
        for (var iterator : fileIterators) {
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
        for (var iterator : fileIterators) {
            try {
                iterator.close();
            } catch (IOException e) {
                throw new IllegalStateException("An error occurred while trying to close the iterator.", e);
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
            throw new IllegalStateException("An error occurred while reading files.", e);
        }
    }

    private void saveData(NavigableMap<MemorySegment, Entry<MemorySegment>> storage) throws IOException {
        Path filePath = basePath.resolve(FILE_NAME + filesCount + FILE_EXTENSION);
        if (!Files.exists(filePath)) {
            Files.createFile(filePath);
        }

        try (FileChannel fileChannel = FileChannel.open(filePath, StandardOpenOption.WRITE)) {
            for (var entry : storage.entrySet()) {
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
        }
    }

    private void saveIndexes(NavigableMap<MemorySegment, Entry<MemorySegment>> storage) throws IOException {
        Path indexPath = basePath.resolve(FILE_INDEX_NAME + filesCount + FILE_EXTENSION);
        if (!Files.exists(indexPath)) {
            Files.createFile(indexPath);
        }

        try (FileChannel fileChannel = FileChannel.open(indexPath, StandardOpenOption.WRITE)) {
            ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
            buffer.putLong(storage.size());
            buffer.flip();
            while (buffer.hasRemaining()) {
                fileChannel.write(buffer);
            }

            buffer.clear();
            long offset = 0;
            for (var entry : storage.entrySet()) {
                Entry<MemorySegment> entryValue = entry.getValue();
                int valueLength = 0;
                if (entryValue.value() != null) {
                    valueLength = (int) entryValue.value().byteSize();
                }

                int keyLength = (int) entryValue.key().byteSize();

                buffer.putLong(offset);
                buffer.flip();
                while (buffer.hasRemaining()) {
                    fileChannel.write(buffer);
                }

                buffer.clear();
                offset += Integer.BYTES + keyLength + Integer.BYTES + valueLength;
            }
        }
    }

    private long getIndexSize(Path indexPath) {
        long size;
        try (FileChannel fileChannel = FileChannel.open(indexPath, StandardOpenOption.READ)) {
            ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
            fileChannel.read(buffer);
            buffer.flip();
            size = buffer.getLong();
        } catch (IOException e) {
            throw new IllegalStateException("An error occurred while reading the file.", e);
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

    static Entry<MemorySegment> getCurrentEntry(long position, FileChannel table,
                                                FileChannel index) throws IOException {
        index.position((position + 1) * Long.BYTES);
        ByteBuffer bufferLong = ByteBuffer.allocate(Long.BYTES);
        index.read(bufferLong);
        bufferLong.flip();
        long offset = bufferLong.getLong();

        table.position(offset);
        ByteBuffer bufferInt = ByteBuffer.allocate(Integer.BYTES);
        table.read(bufferInt);
        bufferInt.flip();
        int keyLength = bufferInt.getInt();
        bufferInt.clear();
        ByteBuffer key = ByteBuffer.allocate(keyLength);
        table.read(key);

        table.read(bufferInt);
        bufferInt.flip();
        int valueLength = bufferInt.getInt();
        if (valueLength == -1) {
            return new BaseEntry<>(MemorySegment.ofArray(key.array()), null);
        }
        ByteBuffer value = ByteBuffer.allocate(valueLength);
        table.read(value);
        return new BaseEntry<>(MemorySegment.ofArray(key.array()), MemorySegment.ofArray(value.array()));
    }
}
