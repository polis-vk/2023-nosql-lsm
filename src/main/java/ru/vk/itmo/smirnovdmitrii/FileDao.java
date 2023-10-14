package ru.vk.itmo.smirnovdmitrii;

import ru.vk.itmo.BaseEntry;
import ru.vk.itmo.Entry;
import ru.vk.itmo.smirnovdmitrii.util.MemorySegmentComparator;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Stream;

public class FileDao implements OutMemoryDao<MemorySegment, Entry<MemorySegment>> {
    private static final Path DEFAULT_BASE_PATH = Path.of("");
    private static final String SS_TABLE_BASE_NAME = "ss_table";
    private static final String OFFSETS_BASE_NAME = "offsets";
    private static final String SS_TABLES_DIRECTORY_NAME = "ss_tables";
    private static final String OFFSETS_DIRECTORY_NAME = "offsets";
    private static final long DELETED_VALUE_SIZE = -1L;
    private final MemorySegmentComparator comparator = MemorySegmentComparator.getInstance();
    private final List<MemorySegment> mappedSsTables = new ArrayList<>();
    private final List<MemorySegment> mappedSsTableOffsets = new ArrayList<>();
    private final Arena arena = Arena.ofShared();
    private final Path basePath;

    public FileDao() {
        this(DEFAULT_BASE_PATH);
    }

    public FileDao(final Path basePath) {
        this.basePath = basePath;
        try {
            Files.createDirectories(getSsTableDirectory());
            Files.createDirectories(getOffsetsDirectory());
        } catch (final IOException e) {
            throw new UncheckedIOException(e);
        }
        final long ssTableCount;
        try (Stream<Path> stream = Files.list(basePath.resolve(SS_TABLES_DIRECTORY_NAME))) {
            ssTableCount = stream.count();
        } catch (final IOException e) {
            throw new UncheckedIOException("can't list files in base path", e);
        }
        for (int i = 1; i <= ssTableCount; i++) {
            try {
                try (FileChannel channel = FileChannel.open(getSsTablePath(i), StandardOpenOption.READ)) {
                    mappedSsTables.add(channel.map(FileChannel.MapMode.READ_ONLY, 0, channel.size(), arena));
                }
                try (FileChannel channel = FileChannel.open(getOffsetsPath(i), StandardOpenOption.READ)) {
                    mappedSsTableOffsets.add(
                            channel.map(FileChannel.MapMode.READ_ONLY, 0, channel.size(), arena));
                }
            } catch (final IOException e) {
                throw new UncheckedIOException("exception while mapping ss tables", e);
            }
        }
    }

    /**
     * Iterator for sstable.
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
            final MemorySegment offsets = mappedSsTableOffsets.get(i);
            final long offset = upperBound(key, storage, offsets);
            if (offset == storage.byteSize()) {
                continue;
            }
            final Entry<MemorySegment> entry = readBlock(storage, offset);
            if (!comparator.equals(key, entry.key())) {
                continue;
            }
            return entry;
        }
        return null;
    }

    private long upperBound(
            final MemorySegment key,
            final MemorySegment storage,
            final MemorySegment offsets
    ) {
        final long offsetsSize = offsets.byteSize();
        long left = -1;
        long right = offsetsSize / Long.BYTES;
        while (left < right - 1) {
            long midst = (left + right) / 2;
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
        return right == offsetsSize / Long.BYTES
                ? storage.byteSize()
                : offsets.get(ValueLayout.JAVA_LONG_UNALIGNED, right * Long.BYTES);
    }

    /*
        Saves storage with one byte block per element. One block is
        JAVA_LONG_UNALIGNED] key_size [JAVA_LONG_UNALIGNED] value_size [bytes] key [bytes] value (without spaces).

        Also saves offsets in same order.
     */
    @Override
    public void save(final Map<MemorySegment, Entry<MemorySegment>> storage) throws IOException {
        Objects.requireNonNull(storage, "storage must be not null");
        if (storage.isEmpty()) {
            return;
        }
        long appendSize = 0;
        final Collection<Entry<MemorySegment>> values = storage.values();
        for (final Entry<MemorySegment> entry : values) {
            appendSize += entry.key().byteSize() + 2L * Long.BYTES;
            final MemorySegment value = entry.value();
            if (value != null) {
                appendSize += value.byteSize();
            }
        }

        final MemorySegment mappedSsTable;
        final StandardOpenOption[] openOptions = {
                StandardOpenOption.CREATE,
                StandardOpenOption.READ,
                StandardOpenOption.WRITE
        };
        final Path newSsTablePath = newSsTablePath();
        try (FileChannel ssTableChannel = FileChannel.open(newSsTablePath, openOptions)) {
            mappedSsTable = ssTableChannel.map(
                    FileChannel.MapMode.READ_WRITE, 0, appendSize, arena);
        } catch (final IOException e) {
            throw new IOException("exception while mapping ss table for read-write.", e);
        }

        final MemorySegment mappedOffsets;
        final Path newSsTableOffsetsPath = newSsTableOffsetsPath();
        try (FileChannel offsetsChannel = FileChannel.open(newSsTableOffsetsPath, openOptions)) {
            mappedOffsets = offsetsChannel.map(
                    FileChannel.MapMode.READ_WRITE, 0, (long) values.size() * Long.BYTES, arena);
        } catch (final IOException e) {
            throw new IOException("exception while mapping offsets for ss table (while saving).", e);
        }

        long ssTableOffset = 0;
        long ssTableOffsetsOffset = 0;
        for (final Entry<MemorySegment> entry : values) {
            mappedOffsets.set(ValueLayout.JAVA_LONG_UNALIGNED, ssTableOffsetsOffset, ssTableOffset);
            ssTableOffsetsOffset += Long.BYTES;
            writeBlock(mappedSsTable, ssTableOffset, entry);
            ssTableOffset += blockSize(entry);
        }
    }

    private Path getSsTableDirectory() {
        return basePath.resolve(SS_TABLES_DIRECTORY_NAME);
    }

    private Path getOffsetsDirectory() {
        return basePath.resolve(OFFSETS_DIRECTORY_NAME);
    }

    private Path getSsTablePath(final int i) {
        return getSsTableDirectory().resolve(SS_TABLE_BASE_NAME + i);
    }

    private Path getOffsetsPath(final int i) {
        return getOffsetsDirectory().resolve(OFFSETS_BASE_NAME + i);
    }

    private Path newSsTablePath() {
        return getSsTablePath(mappedSsTables.size() + 1);
    }

    private Path newSsTableOffsetsPath() {
        return getOffsetsPath(mappedSsTableOffsets.size() + 1);
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
        if (valueSize != DELETED_VALUE_SIZE) {
            value = storage.asSlice(currentOffset, valueSize);
        } else {
            value = null;
        }
        return new BaseEntry<>(key, value);
    }

    @Override
    public List<Iterator<Entry<MemorySegment>>> get(final MemorySegment from, final MemorySegment to) {
        final List<Iterator<Entry<MemorySegment>>> iterators = new ArrayList<>();
        for (int i = mappedSsTables.size() - 1; i >= 0; i--) {
            final MemorySegment ssTable = mappedSsTables.get(i);
            final MemorySegment ssTableOffsets = mappedSsTableOffsets.get(i);
            final long fromOffset = from == null ? 0 : upperBound(from, ssTable, ssTableOffsets);
            final long toOffset = to == null ? ssTable.byteSize() : upperBound(to, ssTable, ssTableOffsets);
            final Iterator<Entry<MemorySegment>> iterator = new SSTableIterator(
                ssTable, fromOffset, toOffset
            );
            iterators.add(iterator);
        }
        return iterators;
    }

    @Override
    public void close() {
        arena.close();
    }
}
