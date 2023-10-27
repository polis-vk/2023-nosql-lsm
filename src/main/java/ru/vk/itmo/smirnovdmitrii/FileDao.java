package ru.vk.itmo.smirnovdmitrii;

import ru.vk.itmo.BaseEntry;
import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;
import ru.vk.itmo.smirnovdmitrii.util.MemorySegmentComparator;

import java.io.IOException;
import java.io.UncheckedIOException;
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
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.UUID;

public class FileDao implements OutMemoryDao<MemorySegment, Entry<MemorySegment>> {
    private static final Path DEFAULT_BASE_PATH = Path.of("");
    private static final long DELETED_VALUE_SIZE = -1L;
    private static final String INDEX_FILE_NAME = "index";
    private final Dao<MemorySegment, Entry<MemorySegment>> dao;
    private final MemorySegmentComparator comparator = new MemorySegmentComparator();
    private List<MemorySegment> mappedSsTables = new ArrayList<>();
    private final Arena arena = Arena.ofShared();
    private final Path basePath;

    public FileDao(final Dao<MemorySegment, Entry<MemorySegment>> dao) {
        this(dao, DEFAULT_BASE_PATH);
    }

    public FileDao(final Dao<MemorySegment, Entry<MemorySegment>> dao, final Path basePath) {
        this.dao = dao;
        this.basePath = basePath;
        try {
            Files.createDirectories(basePath);
        } catch (final IOException e) {
            throw new UncheckedIOException(e);
        }
        try {
            Files.createFile(basePath.resolve(INDEX_FILE_NAME));
        } catch (final FileAlreadyExistsException e) {
            // ok
        } catch (final IOException e) {
            throw new UncheckedIOException(e);
        }
        final List<Path> paths;
        try {
            paths = Files.readAllLines(basePath.resolve(INDEX_FILE_NAME)).stream().map(Path::of).toList();
        } catch (final IOException e) {
            throw new UncheckedIOException("exception while listing base directory.", e);
        }
        for (final Path path: paths) {
            try (FileChannel channel = FileChannel.open(path, StandardOpenOption.READ)) {
                mappedSsTables.add(channel.map(FileChannel.MapMode.READ_ONLY, 0, channel.size(), arena));
            } catch (final IOException e) {
                throw new UncheckedIOException("exception while mapping sstables", e);
            }
        }
    }

    /**
     * Iterator for SSTable.
     */
    private static class SSTableIterator implements Iterator<Entry<MemorySegment>> {
        private final MemorySegment ssTable;
        private final long upperBoundOffset;
        private long offset;

        public SSTableIterator(
                final MemorySegment ssTable,
                final long from,
                final long to
        ) {
            this.ssTable = ssTable;
            this.offset = from;
            this.upperBoundOffset = to;
        }

        @Override
        public boolean hasNext() {
            return offset < upperBoundOffset;
        }

        @Override
        public Entry<MemorySegment> next() {
            if (!hasNext()) {
                throw new NoSuchElementException("No more elements");
            }
            final Entry<MemorySegment> entry = readBlock(ssTable, offset);
            offset += blockSize(entry);
            return entry;
        }
    }

    @Override
    public Entry<MemorySegment> get(final MemorySegment key) {
        Objects.requireNonNull(key);
        for (int i = mappedSsTables.size() - 1; i >= 0; i--) {
            final MemorySegment storage = mappedSsTables.get(i);
            final long offsetsPartSize = storage.get(ValueLayout.JAVA_LONG_UNALIGNED, 0);
            final MemorySegment offsets = storage.asSlice(0, offsetsPartSize);
            final long offset = binarySearch(key, storage, offsets);
            if (offset < 0) {
                continue;
            }
            return readBlock(storage, offset);
        }
        return null;
    }

    /**
     * Searching offset in storage for block with {@code key} using helping file with storage offsets.
     * If there searching key then returns (insert position | 1 << 63).
     * Insert position is position there element would be inserted storage.
     * If key is greater than all element then returns (storage.size() | 1 << 63);
     * {@code offsets}.
     * @param key searching key for upper bound.
     * @param storage sstabe for upper bound.
     * @param offsets helping table with {@code storage} offsets.
     * @return offset in sstable for key block.
     */
    private long binarySearch(
            final MemorySegment key,
            final MemorySegment storage,
            final MemorySegment offsets
    ) {
        final long offsetsSize = offsets.byteSize();
        long left = -1;
        long right = offsetsSize / Long.BYTES;
        while (left < right - 1) {
            long midst = (left + right) >>> 1;
            final long offset = offsets.get(ValueLayout.JAVA_LONG_UNALIGNED, midst * Long.BYTES);
            final long currentKeySize = storage.get(ValueLayout.JAVA_LONG_UNALIGNED, offset);
            final MemorySegment currentKey = storage.asSlice(offset + 2 * Long.BYTES, currentKeySize);
            final int compareResult = comparator.compare(key, currentKey);
            if (compareResult == 0) {
                return offset;
            } else if (compareResult > 0) {
                left = midst;
            } else {
                right = midst;
            }
        }
        final long upperBound = right == offsetsSize / Long.BYTES ? storage.byteSize()
                : offsets.get(ValueLayout.JAVA_LONG_UNALIGNED, right * Long.BYTES);
        return tombstone(upperBound);
    }

    /**
        Saves entries with one byte block per element.
        One block is:
        [JAVA_LONG_UNALIGNED] key_size [JAVA_LONG_UNALIGNED] value_size [bytes] key [bytes] value (without spaces).
        SSTable structure:
        offset of block 1 [JAVA_LONG_UNALIGNED]
        offset of block 2 [JAVA_LONG_UNALIGNED]
     ...
        offset of block n [JAVA_LONG_UNALIGNED]
        block 1
        block 2
     ...
        block n
     */
    @Override
    public synchronized void save(final Iterable<Entry<MemorySegment>> entries) throws IOException {
        Objects.requireNonNull(entries, "entries must be not null");
        long appendSize = 0;
        long count = 0;
        for (final Entry<MemorySegment> entry : entries) {
            count++;
            appendSize += entry.key().byteSize() + 2L * Long.BYTES;
            final MemorySegment value = entry.value();
            if (value != null) {
                appendSize += value.byteSize();
            }
        }
        if (count == 0) {
            return;
        }
        final long offsetsPartSize = count * Long.BYTES;
        appendSize += offsetsPartSize;
        final Path newSsTablePath = newSsTablePath();
        try (Arena savingArena = Arena.ofConfined()) {
            final MemorySegment mappedSsTable;
            try (FileChannel channel = FileChannel.open(
                    newSsTablePath,
                    StandardOpenOption.CREATE,
                    StandardOpenOption.READ,
                    StandardOpenOption.WRITE)
            ) {
                mappedSsTable = channel.map(FileChannel.MapMode.READ_WRITE, 0, appendSize, savingArena);
            }
            long offset = 0;
            long currentEntryOffset = offsetsPartSize;
            for (final Entry<MemorySegment> entry : entries) {
                mappedSsTable.set(ValueLayout.JAVA_LONG_UNALIGNED, offset, currentEntryOffset);
                currentEntryOffset += blockSize(entry);
                offset += Long.BYTES;
            }
            for (final Entry<MemorySegment> entry : entries) {
                writeBlock(mappedSsTable, offset, entry);
                offset += blockSize(entry);
            }
        }
        final Path indexFilePath = basePath.resolve(INDEX_FILE_NAME);
        final List<String> sstables = new ArrayList<>(Files.readAllLines(indexFilePath));
        sstables.add(newSsTablePath.toString());
        changeIndex(sstables);
        try (FileChannel channel = FileChannel.open(newSsTablePath, StandardOpenOption.READ)) {
            mappedSsTables.add(channel.map(FileChannel.MapMode.READ_ONLY, 0, channel.size(), arena));
        }
    }

    private Path newSsTablePath() {
        return basePath.resolve(UUID.randomUUID().toString());
    }

    private static void writeBlock(
            final MemorySegment storage,
            final long offset,
            final Entry<MemorySegment> entry
    ) {
        long currentOffset = offset;
        final MemorySegment key = entry.key();
        final long keySize = key.byteSize();
        storage.set(ValueLayout.JAVA_LONG_UNALIGNED, currentOffset, keySize);
        currentOffset += Long.BYTES;
        final MemorySegment value = entry.value();
        final long valueSize;
        if (value == null) {
            valueSize = DELETED_VALUE_SIZE;
        } else {
            valueSize = value.byteSize();
        }
        storage.set(ValueLayout.JAVA_LONG_UNALIGNED, currentOffset, valueSize);
        currentOffset += Long.BYTES;
        MemorySegment.copy(key, 0, storage, currentOffset, keySize);
        currentOffset += keySize;
        if (value != null) {
            MemorySegment.copy(value, 0, storage, currentOffset, valueSize);
        }
    }

    private static long blockSize(final Entry<MemorySegment> entry) {
        final MemorySegment value = entry.value();
        final long valueSize = value == null ? 0 : value.byteSize();
        return 2 * Long.BYTES + entry.key().byteSize() + valueSize;
    }

    private static Entry<MemorySegment> readBlock(final MemorySegment storage, final long offset) {
        long currentOffset = offset;
        final long keySize = storage.get(ValueLayout.JAVA_LONG_UNALIGNED, currentOffset);
        currentOffset += Long.BYTES;
        final long valueSize = storage.get(ValueLayout.JAVA_LONG_UNALIGNED, currentOffset);
        currentOffset += Long.BYTES;
        final MemorySegment key = storage.asSlice(currentOffset, keySize);
        currentOffset += keySize;
        final MemorySegment value;
        if (valueSize == DELETED_VALUE_SIZE) {
            value = null;
        } else {
            value = storage.asSlice(currentOffset, valueSize);
        }
        return new BaseEntry<>(key, value);
    }

    @Override
    public List<Iterator<Entry<MemorySegment>>> get(final MemorySegment from, final MemorySegment to) {
        final List<Iterator<Entry<MemorySegment>>> iterators = new ArrayList<>();
        for (int i = mappedSsTables.size() - 1; i >= 0; i--) {
            final MemorySegment ssTable = mappedSsTables.get(i);
            final long offsetsPartSize = ssTable.get(ValueLayout.JAVA_LONG_UNALIGNED, 0);
            final MemorySegment offsets = ssTable.asSlice(0, offsetsPartSize);
            final long fromOffset = from == null ? offsetsPartSize : normalize(binarySearch(from, ssTable, offsets));
            final long toOffset = to == null ? ssTable.byteSize() : normalize(binarySearch(to, ssTable, offsets));
            final Iterator<Entry<MemorySegment>> iterator = new SSTableIterator(
                    ssTable, fromOffset, toOffset
            );
            iterators.add(iterator);
        }
        return iterators;
    }

    @Override
    public void compact() throws IOException {
        save(dao::all);
        if (!mappedSsTables.isEmpty()) {
            final List<MemorySegment> newMappedSsTables = new ArrayList<>();
            newMappedSsTables.add(mappedSsTables.getLast());
            mappedSsTables = newMappedSsTables;
            final Path indexFilePath = basePath.resolve(INDEX_FILE_NAME);
            final List<String> sstableNames = Files.readAllLines(indexFilePath);
            changeIndex(List.of(sstableNames.getLast()));
            for (int i = 0; i < sstableNames.size() - 1; i++) {
                Files.delete(basePath.resolve(sstableNames.get(i)));
            }
        }
    }

    private void changeIndex(final List<String> newIndex) throws IOException {
        final Path indexFilePath = basePath.resolve(INDEX_FILE_NAME);
        final Path tmpIndexFilePath = basePath.resolve(INDEX_FILE_NAME + ".tmp");
        Files.write(
                tmpIndexFilePath,
                newIndex,
                StandardOpenOption.CREATE,
                StandardOpenOption.WRITE,
                StandardOpenOption.TRUNCATE_EXISTING
        );
        Files.move(
                tmpIndexFilePath,
                indexFilePath,
                StandardCopyOption.REPLACE_EXISTING,
                StandardCopyOption.ATOMIC_MOVE
        );
    }

    private static long tombstone(final long value) {
        return value | 1L << 63;
    }

    private static long normalize(final long value) {
        return value & ~(1L << 63);
    }

    @Override
    public void close() {
        arena.close();
    }
}
