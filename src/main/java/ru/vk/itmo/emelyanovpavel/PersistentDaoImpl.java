package ru.vk.itmo.emelyanovpavel;

import ru.vk.itmo.BaseEntry;
import ru.vk.itmo.Entry;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;

import static java.lang.foreign.ValueLayout.JAVA_LONG_UNALIGNED;
import static java.nio.channels.FileChannel.MapMode.READ_ONLY;
import static java.nio.channels.FileChannel.MapMode.READ_WRITE;
import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.TRUNCATE_EXISTING;
import static java.nio.file.StandardOpenOption.WRITE;


public class PersistentDaoImpl extends InMemoryDaoImpl {
    private static final String SSTABLE_NAME = "sstable.txt";
    private static final String INDEX_NAME = "indexes.txt";

    private final Path dataPath;
    private final Path indexPath;
    private final MemorySegment mappedData;
    private final MemorySegment mappedIndex;
    private final Arena arena = Arena.ofConfined();

    public PersistentDaoImpl(Path path) throws IOException {
        dataPath = path.resolve(SSTABLE_NAME);
        indexPath = path.resolve(INDEX_NAME);

        if (!Files.exists(dataPath)) {
            this.mappedData = null;
            this.mappedIndex = null;
            return;
        }

        try (FileChannel dataChanel = FileChannel.open(dataPath, READ)) {
            try (FileChannel indexChanel = FileChannel.open(indexPath, READ)) {
                mappedData = dataChanel.map(READ_ONLY, 0, Files.size(dataPath), arena);
                mappedIndex = indexChanel.map(READ_ONLY, 0, Files.size(indexPath), arena);
            }
        }
    }

    @Override
    public void close() throws IOException {
        if (arena.scope().isAlive()) {
            arena.close();
        }
        flush();
    }

    @Override
    public Entry<MemorySegment> get(MemorySegment key) {
        if (storage.containsKey(key)) {
            return super.get(key);
        }
        if (mappedData == null) {
            return null;
        }
        return getFromSSTable(key);
    }

    private Entry<MemorySegment> getFromSSTable(MemorySegment key) {
        long left = 0;
        long right = getRightLimit();

        while (left <= right) {
            long mid = ((right - left) >>> 1) + left;

            long offset = mappedIndex.get(JAVA_LONG_UNALIGNED, mid * Long.BYTES);

            MemorySegment currentKey = getMappedData(offset);
            offset += currentKey.byteSize() + Long.BYTES;

            int diff = comparator.compare(currentKey, key);
            if (diff == 0) {
                return new BaseEntry<>(currentKey, getMappedData(offset));
            }
            if (diff < 0) {
                left = mid + 1;
            } else {
                right = mid - 1;
            }
        }
        return null;
    }

    @Override
    public void flush() throws IOException {
        if (storage.isEmpty()) {
            return;
        }

        long indexesSize = (long) storage.size() * Long.BYTES;
        long storageSize = calculateCurrentStorageSize();

        try (FileChannel dataChanel = FileChannel.open(dataPath, TRUNCATE_EXISTING, CREATE, WRITE, READ)) {
            try (FileChannel indexChanel = FileChannel.open(indexPath, TRUNCATE_EXISTING, CREATE, WRITE, READ)) {
                try (Arena arenaForWriting = Arena.ofConfined()) {
                    MemorySegment dataSegmentSaver = dataChanel.map(READ_WRITE, 0, storageSize, arenaForWriting);
                    MemorySegment indexSegmentSaver = indexChanel.map(READ_WRITE, 0, indexesSize, arenaForWriting);
                    long indexOffset = 0;
                    long dataOffset = 0;
                    for (Entry<MemorySegment> entry : storage.values()) {
                        var key = entry.key();
                        var value = entry.value();

                        indexSegmentSaver.set(JAVA_LONG_UNALIGNED, indexOffset, dataOffset);
                        indexOffset += Long.BYTES;

                        dataOffset = getOffsetAfterInsertion(key, dataSegmentSaver, dataOffset);
                        dataOffset = getOffsetAfterInsertion(value, dataSegmentSaver, dataOffset);
                    }
                }

            }
        }
    }

    private long getOffsetAfterInsertion(MemorySegment dataToInsert, MemorySegment segment, long currentOffset) {
        long resultedOffset = currentOffset;
        long dataSize = dataToInsert.byteSize();

        segment.set(ValueLayout.JAVA_LONG_UNALIGNED, resultedOffset, dataSize);
        resultedOffset += Long.BYTES;
        segment.asSlice(resultedOffset, dataSize).copyFrom(dataToInsert);
        resultedOffset += dataSize;
        return resultedOffset;
    }

    private long calculateCurrentStorageSize() {
        return storage.values()
                .stream()
                .mapToLong(entry -> entry.key().byteSize() + entry.value().byteSize())
                .sum() + 2L * storage.size() * Long.BYTES;
    }

    private MemorySegment getMappedData(long offset) {
        long copyOfOffset = offset;
        long currentKeySize = mappedData.get(JAVA_LONG_UNALIGNED, copyOfOffset);
        copyOfOffset += Long.BYTES;
        return mappedData.asSlice(copyOfOffset, currentKeySize);
    }

    private long getRightLimit() {
        return mappedIndex.byteSize() / Long.BYTES;
    }
}
