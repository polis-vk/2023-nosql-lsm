package ru.vk.itmo.reference;

import ru.vk.itmo.BaseEntry;
import ru.vk.itmo.Entry;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Iterator;

/**
 * Persistent SSTable in data file and index file.
 *
 * @see SSTables
 * @author incubos
 */
public final class SSTable implements ReadableTable {
    public final int sequence;
    public final Path indexName;
    public final Path dataName;

    private final MemorySegment index;
    private final MemorySegment data;
    private final long size;

    public SSTable(
            final Arena arena,
            final Path baseDir,
            final int sequence) throws IOException {
        // Build file names
        this.sequence = sequence;
        this.indexName = SSTables.indexName(baseDir, sequence);
        this.dataName = SSTables.dataName(baseDir, sequence);

        // Open segments
        this.index = mapReadOnly(arena, indexName);
        this.data = mapReadOnly(arena, dataName);
        this.size = index.byteSize() / Long.BYTES;
    }

    private static MemorySegment mapReadOnly(
            final Arena arena,
            final Path path) throws IOException {
        try (final FileChannel channel =
                     FileChannel.open(
                             path,
                             StandardOpenOption.READ)) {
            return channel.map(
                    FileChannel.MapMode.READ_ONLY,
                    0L,
                    Files.size(path),
                    arena);
        }
    }

    /**
     * @return index of the entry if found; otherwise, (-(insertion point) - 1).
     * The insertion point is defined as the point at which the key would be inserted:
     * the index of the first element greater than the key,
     * or size if all keys are less than the specified key.
     * Note that this guarantees that the return value will be >= 0
     * if and only if the key is found.
     */
    private long entryBinarySearch(final MemorySegment key) {
        long low = 0L;
        long high = size - 1;

        while (low <= high) {
            final long mid = (low + high) >>> 1;
            final long midEntryOffset = entryOffset(mid);
            final long midKeyLength = getLength(midEntryOffset);
            final int compare =
                    MemorySegmentComparator.compare(
                            data,
                            midEntryOffset + Long.BYTES, // Position at key
                            midKeyLength,
                            key,
                            0L,
                            key.byteSize());

            if (compare < 0) {
                low = mid + 1;
            } else if (compare > 0) {
                high = mid - 1;
            } else {
                return mid;
            }
        }

        return -(low + 1);
    }

    private long entryOffset(final long entry) {
        return index.get(
                        ValueLayout.OfLong.JAVA_LONG,
                        entry * Long.BYTES);
    }

    private long getLength(final long offset) {
        return data.get(
                ValueLayout.OfLong.JAVA_LONG,
                offset);
    }

    @Override
    public Iterator<Entry<MemorySegment>> get(
            final MemorySegment from,
            final MemorySegment to) {
        throw new UnsupportedOperationException("Not implemented (yet)!");
    }

    @Override
    public Entry<MemorySegment> get(final MemorySegment key) {
        final long entry = entryBinarySearch(key);
        if (entry < 0) {
            return null;
        }

        // Extract key
        long offset = entryOffset(entry);
        final long keyLength = getLength(offset);
        // Skip key (will reuse the argument)
        offset += Long.BYTES + keyLength;
        // Extract value length
        final long valueLength = getLength(offset);
        if (valueLength == SSTables.TOMBSTONE_VALUE_LENGTH) {
            // Tombstone encountered
            return new BaseEntry<>(key, null);
        } else {
            // Get value
            offset += Long.BYTES;
            final MemorySegment value = data.asSlice(offset, valueLength);
            return new BaseEntry<>(key, value);
        }
    }
}
