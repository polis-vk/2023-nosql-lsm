package ru.vk.itmo.emelyanovpavel;

import ru.vk.itmo.BaseEntry;
import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

import static java.lang.foreign.ValueLayout.JAVA_BYTE;
import static java.lang.foreign.ValueLayout.JAVA_LONG_UNALIGNED;
import static java.nio.channels.FileChannel.MapMode.READ_WRITE;

public class PersistentDaoImpl implements Dao<MemorySegment, Entry<MemorySegment>> {
    private static final String SSTABLE_NAME = "sstable.txt";
    private static final String INDEX_NAME = "indexes.txt";

    private static final Set<OpenOption> WRITE_OPTIONS = Set.of(
            StandardOpenOption.READ,
            StandardOpenOption.WRITE,
            StandardOpenOption.CREATE,
            StandardOpenOption.TRUNCATE_EXISTING
    );

    private final Path dataPath;
    private final Path indexPath;
    private final MemorySegment mappedData;
    private final MemorySegment mappedIndex;
    private final Arena arena = Arena.ofConfined();
    private final ConcurrentNavigableMap<MemorySegment, Entry<MemorySegment>> storage;

    public PersistentDaoImpl(Path path) throws IOException {
        storage = new ConcurrentSkipListMap<>(new MemorySegmentComparator());
        dataPath = path.resolve(SSTABLE_NAME);
        indexPath = path.resolve(INDEX_NAME);

        if (!Files.exists(dataPath) || !Files.exists(indexPath)) {
            this.mappedData = null;
            this.mappedIndex = null;
            return;
        }

        try (FileChannel dataChanel = FileChannel.open(dataPath, StandardOpenOption.READ)) {
            try (FileChannel indexChanel = FileChannel.open(indexPath, StandardOpenOption.READ)) {
                mappedData = dataChanel.map(FileChannel.MapMode.READ_ONLY, 0, Files.size(dataPath), arena);
                mappedIndex = indexChanel.map(FileChannel.MapMode.READ_ONLY, 0, Files.size(indexPath), arena);
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
    public Iterator<Entry<MemorySegment>> get(MemorySegment from, MemorySegment to) {
        if (from == null && to == null) {
            return all();
        }
        if (from == null) {
            return allTo(to);
        }
        if (to == null) {
            return allFrom(from);
        }
        return storage.subMap(from, to)
                .values()
                .iterator();
    }

    @Override
    public Iterator<Entry<MemorySegment>> allFrom(MemorySegment from) {
        return storage.tailMap(from)
                .values()
                .iterator();
    }

    @Override
    public Iterator<Entry<MemorySegment>> allTo(MemorySegment to) {
        return storage.headMap(to)
                .values()
                .iterator();
    }

    @Override
    public Iterator<Entry<MemorySegment>> all() {
        return storage.values()
                .iterator();
    }

    @Override
    public Entry<MemorySegment> get(MemorySegment key) {
        Entry<MemorySegment> entryFromStorage = storage.get(key);
        if (entryFromStorage != null) {
            return entryFromStorage;
        }
        if (mappedData == null) {
            return null;
        }
        return getFromSSTable(key);
    }

    @Override
    public void upsert(Entry<MemorySegment> entry) {
        storage.put(entry.key(), entry);
    }

    private Entry<MemorySegment> getFromSSTable(MemorySegment key) {
        long left = 0;
        long right = getRightLimit();

        while (left <= right) {
            long mid = ((right - left) >>> 1) + left;
            long offset = mappedIndex.get(JAVA_LONG_UNALIGNED, mid * Long.BYTES);

            long currentKeySize = mappedData.get(JAVA_LONG_UNALIGNED, offset);

            long srcKeySize = key.byteSize();
            if (currentKeySize > srcKeySize) {
                right = mid - 1;
                continue;
            }
            if (currentKeySize < srcKeySize) {
                left = mid + 1;
                continue;
            }
            offset += Long.BYTES + currentKeySize;
            long mismatch = MemorySegment.mismatch(mappedData, offset - currentKeySize, offset, key, 0, srcKeySize);
            if (mismatch == -1) {
                return new BaseEntry<>(key, getMappedData(offset));
            }

            long indexOfMismatchByteInSStable = offset - currentKeySize + mismatch;
            byte mismatchByteInSStable = mappedData.get(JAVA_BYTE, indexOfMismatchByteInSStable);
            int diff = Byte.compare(mismatchByteInSStable, key.get(JAVA_BYTE, mismatch));
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

        try (FileChannel dataChanel = FileChannel.open(dataPath, WRITE_OPTIONS)) {
            try (FileChannel indexChanel = FileChannel.open(indexPath, WRITE_OPTIONS)) {
                try (Arena arenaForWriting = Arena.ofConfined()) {
                    MemorySegment dataSegmentSaver = dataChanel.map(READ_WRITE, 0, storageSize, arenaForWriting);
                    MemorySegment indexSegmentSaver = indexChanel.map(READ_WRITE, 0, indexesSize, arenaForWriting);
                    long indexOffset = 0;
                    long dataOffset = 0;
                    for (Entry<MemorySegment> entry : storage.values()) {
                        indexSegmentSaver.set(JAVA_LONG_UNALIGNED, indexOffset, dataOffset);
                        indexOffset += Long.BYTES;

                        writeDataToMemorySegment(entry.key(), dataSegmentSaver, dataOffset);
                        dataOffset = getNextOffsetAfterInsertion(entry.key(), dataOffset);

                        writeDataToMemorySegment(entry.value(), dataSegmentSaver, dataOffset);
                        dataOffset = getNextOffsetAfterInsertion(entry.value(), dataOffset);
                    }
                }
            }
        }
    }

    private void writeDataToMemorySegment(MemorySegment dataToInsert, MemorySegment segment, long currentOffset) {
        long dataSize = dataToInsert.byteSize();
        segment.set(JAVA_LONG_UNALIGNED, currentOffset, dataSize);
        MemorySegment.copy(dataToInsert, 0, segment, currentOffset + Long.BYTES, dataSize);
    }

    private long getNextOffsetAfterInsertion(MemorySegment dataToInsert, long currentOffset) {
        return currentOffset + Long.BYTES + dataToInsert.byteSize();
    }

    private long calculateCurrentStorageSize() {
        return getAmountOfBytesToStoreKeyAndValue() + getAmountOfBytesToStoreKeyAndValueSize();
    }

    private long getAmountOfBytesToStoreKeyAndValueSize() {
        return 2L * storage.size() * Long.BYTES;
    }

    private long getAmountOfBytesToStoreKeyAndValue() {
        return storage.values()
                .stream()
                .mapToLong(entry -> entry.key().byteSize() + entry.value().byteSize())
                .sum();
    }

    private MemorySegment getMappedData(long offset) {
        long copyOfOffset = offset;
        long currentKeySize = mappedData.get(JAVA_LONG_UNALIGNED, copyOfOffset);
        copyOfOffset += Long.BYTES;
        return mappedData.asSlice(copyOfOffset, currentKeySize);
    }

    private long getRightLimit() {
        return mappedIndex.byteSize() / Long.BYTES - 1;
    }
}
