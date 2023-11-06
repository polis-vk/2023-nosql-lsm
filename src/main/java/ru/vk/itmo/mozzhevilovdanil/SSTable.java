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
import java.nio.file.StandardOpenOption;
import java.util.Iterator;
import java.util.NoSuchElementException;

import static java.lang.foreign.ValueLayout.JAVA_LONG_UNALIGNED;
import static java.util.Collections.emptyIterator;
import static ru.vk.itmo.mozzhevilovdanil.DatabaseUtils.binSearch;

public class SSTable {
    private final MemorySegment readPage;
    private final MemorySegment readIndex;

    private boolean isCreated;

    // comment to pass stage 3
    SSTable(Arena arena, Config config, long tableIndex) throws IOException {
        Path tablePath = config.basePath().resolve(tableIndex + ".db");
        Path indexPath = config.basePath().resolve(tableIndex + ".index.db");

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

        MemorySegment pageCurrent = getMemorySegment(size, tablePath, arena);
        MemorySegment indexCurrent = getMemorySegment(indexSize, indexPath, arena);

        readPage = pageCurrent;
        readIndex = indexCurrent;
    }

    private MemorySegment getMemorySegment(long size, Path path, Arena arena) throws IOException {
        MemorySegment currentMemorySegment;
        try (FileChannel fileChannel = FileChannel.open(path, StandardOpenOption.READ)) {
            currentMemorySegment = fileChannel.map(FileChannel.MapMode.READ_ONLY, 0, size, arena);
            isCreated = true;
        } catch (FileNotFoundException e) {
            currentMemorySegment = null;
        }
        return currentMemorySegment;
    }

    public Entry<MemorySegment> get(MemorySegment key) {
        if (readPage == null) {
            return null;
        }

        long result = binSearch(readIndex, readPage, key);
        if (result == readIndex.byteSize()) {
            return null;
        }
        long offset = readIndex.get(JAVA_LONG_UNALIGNED, result);
        long resultCompare = DatabaseUtils.compareInPlace(readPage, offset, key);
        if (resultCompare != 0) {
            return null;
        }
        return entryAtOffset(offset);
    }

    public Iterator<Entry<MemorySegment>> get(MemorySegment from, MemorySegment to) {
        if (readPage == null) {
            return emptyIterator();
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
                    throw new NoSuchElementException("next on empty iterator");
                }
                Entry<MemorySegment> entry = entryAtPosition(currentOffset);
                currentOffset += Long.BYTES;
                return entry;
            }
        };
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
        MemorySegment value = null;
        if (valueSize != -1) {
            value = readPage.asSlice(innerOffset, valueSize);
        }
        return new BaseEntry<>(key, value);
    }

    public boolean isCreated() {
        return isCreated;
    }
}
