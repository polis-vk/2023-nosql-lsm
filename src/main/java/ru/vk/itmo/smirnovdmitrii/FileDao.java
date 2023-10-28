package ru.vk.itmo.smirnovdmitrii;

import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;
import ru.vk.itmo.smirnovdmitrii.util.MemorySegmentComparator;
import ru.vk.itmo.smirnovdmitrii.util.SSTableUtil;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.channels.FileChannel;
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
        final Path indexFilePath = basePath.resolve(INDEX_FILE_NAME);
        try {
            if (!Files.exists(indexFilePath)) {
                Files.createFile(indexFilePath);
            }
        } catch (final IOException e) {
            throw new UncheckedIOException(e);
        }
        final List<String> paths;
        try {
            paths = Files.readAllLines(indexFilePath);
        } catch (final IOException e) {
            throw new UncheckedIOException("exception while reading index file.", e);
        }
        for (final String path: paths) {
            try (FileChannel channel = FileChannel.open(Path.of(path), StandardOpenOption.READ)) {
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
            final Entry<MemorySegment> entry = SSTableUtil.readBlock(ssTable, offset);
            offset++;
            return entry;
        }
    }

    @Override
    public Entry<MemorySegment> get(final MemorySegment key) {
        Objects.requireNonNull(key);
        for (int i = mappedSsTables.size() - 1; i >= 0; i--) {
            final MemorySegment storage = mappedSsTables.get(i);
            final long offset = binarySearch(key, storage);
            if (offset >= 0) {
                return SSTableUtil.readBlock(storage, offset);
            }
        }
        return null;
    }

    /**
     * Searching order number in storage for block with {@code key} using helping file with storage offsets.
     * If there is no block with such key, returns -(insert position + 1).
     * {@code offsets}.
     * @param key searching key.
     * @param storage sstable.
     * @return offset in sstable for key block.
     */
    private long binarySearch(
            final MemorySegment key,
            final MemorySegment storage
    ) {
        long left = -1;
        long right = SSTableUtil.blockCount(storage);
        while (left < right - 1) {
            long midst = (left + right) >>> 1;
            final MemorySegment currentKey = SSTableUtil.readBlockKey(storage, midst);
            final int compareResult = comparator.compare(key, currentKey);
            if (compareResult == 0) {
                return midst;
            } else if (compareResult > 0) {
                left = midst;
            } else {
                right = midst;
            }
        }
        return SSTableUtil.tombstone(right);
    }

    private long upperBound(
            final MemorySegment key,
            final MemorySegment storage
    ) {
        final long result = binarySearch(key, storage);
        return SSTableUtil.normalize(result);
    }

    /**
        Saves entries with one byte block per element.
        One block is:
        [bytes] key [bytes] value.
        One meta block is :
        [JAVA_LONG_UNALIGNED] key_offset [JAVA_LONG_UNALIGNED] value_offset
        SSTable structure:
        meta block 1
        meta block 2
     ...
        meta block n
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
            appendSize += entry.key().byteSize();
            final MemorySegment value = entry.value();
            if (value != null) {
                appendSize += value.byteSize();
            }
        }
        if (count == 0) {
            return;
        }
        final long offsetsPartSize = count * Long.BYTES * 2;
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
            long indexOffset = 0;
            long blockOffset = offsetsPartSize;
            for (final Entry<MemorySegment> entry : entries) {
                mappedSsTable.set(ValueLayout.JAVA_LONG_UNALIGNED, indexOffset, blockOffset);
                indexOffset += Long.BYTES;
                final MemorySegment key = entry.key();
                final long keySize = key.byteSize();
                MemorySegment.copy(key, 0, mappedSsTable, blockOffset, keySize);
                final MemorySegment value = entry.value();
                blockOffset += keySize;
                if (value == null) {
                    mappedSsTable.set(ValueLayout.JAVA_LONG_UNALIGNED, indexOffset, SSTableUtil.tombstone(blockOffset));
                } else {
                    mappedSsTable.set(ValueLayout.JAVA_LONG_UNALIGNED, indexOffset, blockOffset);
                    final long valueSize = value.byteSize();
                    MemorySegment.copy(value, 0, mappedSsTable, blockOffset, valueSize);
                    blockOffset += valueSize;
                }
                indexOffset += Long.BYTES;
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

    @Override
    public List<Iterator<Entry<MemorySegment>>> get(final MemorySegment from, final MemorySegment to) {
        final List<Iterator<Entry<MemorySegment>>> iterators = new ArrayList<>();
        for (int i = mappedSsTables.size() - 1; i >= 0; i--) {
            final MemorySegment ssTable = mappedSsTables.get(i);
            final long fromOffset = from == null ? 0 : upperBound(from, ssTable);
            final long toOffset = to == null ? SSTableUtil.blockCount(ssTable) : upperBound(to, ssTable);
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

    @Override
    public void close() {
        if (arena.scope().isAlive()) {
            arena.close();
        }
    }
}
