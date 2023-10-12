package ru.vk.itmo.smirnovdmitrii;

import ru.vk.itmo.BaseEntry;
import ru.vk.itmo.Entry;
import ru.vk.itmo.smirnovdmitrii.util.MemorySegmentComparator;
import ru.vk.itmo.smirnovdmitrii.util.PeekingIterator;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.channels.FileChannel;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class FileDao implements OutMemoryDao<MemorySegment, Entry<MemorySegment>> {
    private static final Path DEFAULT_BASE_PATH = Path.of("");
    private static final String SS_TABLE_BASE_NAME = "ss_table";
    private static final String SS_TABLE_OFFSETS_BASE_NAME = "ss_table_offsets";
    private static final long DELETED_VALUE_SIZE = -1;
    private final MemorySegmentComparator comparator = MemorySegmentComparator.getInstance();
    private final List<MemorySegment> mappedSsTables = new ArrayList<>();
    private final List<MemorySegment> mappedSsTableOffsets = new ArrayList<>();
    private final Arena arena = Arena.ofShared();

    public FileDao() {
        this(DEFAULT_BASE_PATH);
    }

    public FileDao(final Path basePath) {
        try {
            Files.walkFileTree(basePath, new SimpleFileVisitor<Path>() {
                @Override
                public FileVisitResult visitFile(final Path file, final BasicFileAttributes attrs) throws IOException {
                    try (FileChannel channel = FileChannel.open(file, StandardOpenOption.READ)) {
                        final String fileName = file.getFileName().toString();
                        final MemorySegment segment = channel.map(
                                FileChannel.MapMode.READ_ONLY, 0, channel.size(), arena);
                        if (fileName.startsWith(SS_TABLE_OFFSETS_BASE_NAME)) {
                            mappedSsTableOffsets.add(segment);
                        } else if (fileName.startsWith(SS_TABLE_BASE_NAME)) {
                            mappedSsTables.add(segment);
                        } else {
                            throw new AssertionError("unexpected file");
                        }
                    }
                    return FileVisitResult.CONTINUE;
                }

                @Override
                public FileVisitResult visitFileFailed(final Path file, final IOException exc) throws IOException {
                    throw new IOException(exc);
                }

            });
        } catch (final IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public static class SSTableIterator implements Iterator<Entry<MemorySegment>> {
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
        for (int i = 0; i < mappedSsTables.size(); i++) {
            final MemorySegment storage = mappedSsTables.get(i);
            final MemorySegment offsets = mappedSsTableOffsets.get(i);
            final long offset = binarySearch(key, storage, offsets, true);
            if (offset == -1) {
                continue;
            }
            final Entry<MemorySegment> entry = readBlock(storage, offset);
            if (entry.value() == null) {
                return null;
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
        return binarySearch(key, storage, offsets, false);
    }

    private long binarySearch(
            final MemorySegment key,
            final MemorySegment storage,
            final MemorySegment offsets,
            final boolean accurate
    ) {

        long left = -1;
        long right = offsets.byteSize() / Long.BYTES;
        while (left < right - 1) {
            long midst = (left + right) / 2;
            long offset = offsets.get(ValueLayout.JAVA_LONG_UNALIGNED, midst * Long.BYTES);

            final long currentKeySize = storage.get(ValueLayout.JAVA_LONG_UNALIGNED, offset);
            offset += Long.BYTES;
            final MemorySegment currentKey = storage.asSlice(offset, currentKeySize);

            final int compareResult = comparator.compare(key, currentKey);
            if (compareResult == 0) {
                return midst;
            } else if (compareResult >= 0) {
                left = midst;
            } else {
                right = midst;
            }
        }
        if (accurate) {
            return -1;
        } else {
            return left == -1 ? 0 : left;
        }
    }

    /*
        Saves storage with one byte block per element. One block is
        [JAVA_LONG_UNALIGNED] key_size [bytes] key [JAVA_LONG_UNALIGNED] value_size [bytes] value (without spaces).

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
            appendSize += entry.value().byteSize() + entry.key().byteSize() + 2L * Long.BYTES;
        }

        final MemorySegment mappedSsTable;
        final StandardOpenOption[] openOptions = {
                StandardOpenOption.READ,
                StandardOpenOption.WRITE
        };
        final Path newSsTablePath = Path.of(newSsTableName());
        try (FileChannel ssTableChannel = FileChannel.open(newSsTablePath, openOptions)) {
            mappedSsTable = ssTableChannel.map(
                    FileChannel.MapMode.READ_WRITE, 0, appendSize, arena);
        } catch (final IOException e) {
            throw new IOException("exception while mapping ss table for read-write.", e);
        }

        final MemorySegment mappedOffsets;
        final Path newSsTableOffsetsPath = Path.of(newSsTableOffsetsName());
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

    private String newSsTableName() {
        return SS_TABLE_BASE_NAME + (mappedSsTables.size() + 1);
    }

    private String newSsTableOffsetsName() {
        return SS_TABLE_OFFSETS_BASE_NAME + (mappedSsTableOffsets.size() + 1);
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
        final long valueSize = value.byteSize();
        storage.set(ValueLayout.JAVA_LONG_UNALIGNED, currentOffset, valueSize);
        currentOffset += Long.BYTES;
        MemorySegment.copy(key, 0, storage, currentOffset, keySize);
        currentOffset += keySize;
        MemorySegment.copy(value, 0, storage, currentOffset, valueSize);
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
            value = storage.asSlice(currentOffset, valueSize);
        } else {
            value = null;
        }
        return new BaseEntry<>(key, value);
    }

    @Override
    public List<Iterator<Entry<MemorySegment>>> get(final MemorySegment from, final MemorySegment to) {
        final List<Iterator<Entry<MemorySegment>>> iterators = new ArrayList<>();
        for (int i = 0; i < mappedSsTables.size(); i++) {
            final MemorySegment ssTable = mappedSsTables.get(i);
            final MemorySegment ssTableOffsets = mappedSsTableOffsets.get(i);
            final long fromOffset = upperBound(ssTable, ssTableOffsets, from);
            final long toOffset = upperBound(ssTable, ssTableOffsets, to);
            final Iterator<Entry<MemorySegment>> iterator = new SSTableIterator(
                ssTable, fromOffset, toOffset
            );
            if (iterator.hasNext()) {
                iterators.add(iterator);
            }
        }
        return iterators;
    }

    @Override
    public void close() {
        arena.close();
    }
}
