package ru.vk.itmo.mozzhevilovdanil;

import ru.vk.itmo.BaseEntry;
import ru.vk.itmo.Config;
import ru.vk.itmo.Entry;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.Iterator;
import java.util.SortedMap;
import java.util.UUID;

import static java.lang.foreign.ValueLayout.JAVA_LONG_UNALIGNED;
import static ru.vk.itmo.mozzhevilovdanil.DatabaseUtils.binSearch;

public class SSTable {
    private final Arena arena;
    private final MemorySegment readPage;
    private final MemorySegment readIndex;
    private final Path tablePath;
    private final Path indexPath;
    private final Config config;

    SSTable(Config config, String sstableName) throws IOException {
        this.tablePath = config.basePath().resolve(sstableName);
        this.indexPath = config.basePath().resolve("index.db");
        this.config = config;

        arena = Arena.ofShared();

        long size;
        long indexSize;
        try {
            size = Files.size(tablePath);
            indexSize = Files.size(indexPath);
        } catch (NoSuchFileException e) {
            readPage = null;
            readIndex = null;
            return;
        }


        MemorySegment pageCurrent = getMemorySegment(size, tablePath);
        MemorySegment indexCurrent = getMemorySegment(indexSize, indexPath);

        readPage = pageCurrent;
        readIndex = indexCurrent;
    }

    public Entry<MemorySegment> get(MemorySegment key) {
        if (readPage == null) {
            return null;
        }

        long result = binSearch(readIndex, readPage, key);
        if (result >= readIndex.byteSize()) {
            return null;
        }
        long offset = readIndex.get(JAVA_LONG_UNALIGNED, result);
        long resultCompare = DatabaseUtils.compareInPlace(readPage, offset, key);
        if (resultCompare != 0) {
            return null;
        }
        return entryAtOffset(offset);
    }

    private MemorySegment getMemorySegment(long size, Path path) throws IOException {
        boolean created = false;
        MemorySegment pageCurrent;
        try (FileChannel fileChannel = FileChannel.open(path, StandardOpenOption.READ)) {
            pageCurrent = fileChannel.map(FileChannel.MapMode.READ_ONLY, 0, size, arena);
            created = true;
        } catch (FileNotFoundException e) {
            pageCurrent = null;
        } finally {
            if (!created) {
                arena.close();
            }
        }
        return pageCurrent;
    }

    public Iterator<Entry<MemorySegment>> get(MemorySegment from, MemorySegment to) {
        if (readPage == null) {
            return new Iterator<>() {
                @Override
                public boolean hasNext() {
                    return false;
                }

                @Override
                public Entry<MemorySegment> next() {
                    return null;
                }
            };
        }

        long left = from == null ? 0 : binSearch(readIndex, readPage, from);
        long right = to == null ? readIndex.byteSize() : binSearch(readIndex, readPage, to);

        return new Iterator<>() {
            long currentOffset = left;

            @Override
            public boolean hasNext() {
                return currentOffset < right;
            }

            @Override
            public Entry<MemorySegment> next() {
                if (!hasNext()) {
                    return null;
                }
                Entry<MemorySegment> entry = entryAtPosition(currentOffset);
                currentOffset += Long.BYTES;
                return entry;
            }
        };
    }

    void store(SortedMap<MemorySegment, Entry<MemorySegment>> storage) throws IOException {
        Iterator<Entry<MemorySegment>> mergeIterator = DatabaseUtils.mergeIterator(
                storage.values().iterator(),
                get(null, null)
        );

        String randomTempPrefix = UUID.randomUUID().toString();
        Path tempTablePath = config.basePath().resolve(randomTempPrefix + ".db");
        Path tempIndexPath = config.basePath().resolve(randomTempPrefix + ".index.db");

        long size = 0;
        long indexSize = 0;
        while (mergeIterator.hasNext()) {
            Entry<MemorySegment> entry = mergeIterator.next();
            if (entry.value() == null) {
                continue;
            }
            size += entry.key().byteSize() + entry.value().byteSize() + 2L * Long.BYTES;
            indexSize += Long.BYTES;
        }

        mergeIterator = DatabaseUtils.mergeIterator(
                storage.values().iterator(),
                get(null, null)
        );

        try (Arena writeArena = Arena.ofConfined()) {
            MemorySegment page;
            try (FileChannel fileChannel = getFileChannel(tempTablePath)) {
                page = fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, size, writeArena);
            }
            MemorySegment index;
            try (FileChannel fileChannel = getFileChannel(tempIndexPath)) {
                index = fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, indexSize, writeArena);
            }

            long offset = 0;
            long indexOffset = 0;

            while (mergeIterator.hasNext()) {
                Entry<MemorySegment> entry = mergeIterator.next();
                MemorySegment key = entry.key();

                index.set(JAVA_LONG_UNALIGNED, indexOffset, offset);
                indexOffset += Long.BYTES;

                page.set(JAVA_LONG_UNALIGNED, offset, key.byteSize());
                offset += Long.BYTES;

                MemorySegment value = entry.value();

                page.set(JAVA_LONG_UNALIGNED, offset, value.byteSize());
                offset += Long.BYTES;

                MemorySegment.copy(key, 0, page, offset, key.byteSize());
                offset += key.byteSize();
                MemorySegment.copy(value, 0, page, offset, value.byteSize());
                offset += value.byteSize();
            }

            arena.close();

            Files.move(tempTablePath, tablePath, StandardCopyOption.ATOMIC_MOVE);
            Files.move(tempIndexPath, indexPath, StandardCopyOption.ATOMIC_MOVE);
        }
    }

    private static FileChannel getFileChannel(Path tempIndexPath) throws IOException {
        return FileChannel.open(tempIndexPath,
                StandardOpenOption.WRITE,
                StandardOpenOption.READ,
                StandardOpenOption.TRUNCATE_EXISTING,
                StandardOpenOption.CREATE);
    }

    public Entry<MemorySegment> entryAtPosition(long position) {
        if (position >= readIndex.byteSize()) {
            return null;
        }
        long offset = readIndex.get(JAVA_LONG_UNALIGNED, position);
        return entryAtOffset(offset);
    }

    public Entry<MemorySegment> entryAtOffset(long offset) {
        long innerOffset = offset;
        long keySize = readPage.get(JAVA_LONG_UNALIGNED, innerOffset);
        innerOffset += Long.BYTES;
        long valueSize = readPage.get(JAVA_LONG_UNALIGNED, innerOffset);
        innerOffset += Long.BYTES;
        MemorySegment key = readPage.asSlice(innerOffset, keySize);
        innerOffset += keySize;
        MemorySegment value = readPage.asSlice(innerOffset, valueSize);
        return new BaseEntry<>(key, value);
    }
}
