package ru.vk.itmo.reference;

import ru.vk.itmo.Entry;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.Iterator;

/**
 * Provides {@link SSTable} management facilities: dumping and discovery.
 * <p>
 * Index file {@code <N>.index} contains {@code long} offsets to entries in data file:
 * {@code [offset0, offset1, ...]}
 * <p>
 * Data file {@code <N>.data} contains serialized entries:
 * {@code <long keyLength><key><long valueLength><value>}
 * <p>
 * Tombstones are encoded as {@code valueLength} {@code -1} and no subsequent value.
 *
 * @author incubos
 */
public final class SSTables {
    public static final String INDEX_SUFFIX = ".index";
    public static final String DATA_SUFFIX = ".data";
    public static final long TOMBSTONE_VALUE_LENGTH = -1L;

    private static final String TEMP_SUFFIX = ".tmp";

    private SSTables() {
        // Only static methods
    }

    public static Path indexName(
            final Path baseDir,
            final int sequence) {
        return baseDir.resolve(sequence + INDEX_SUFFIX);
    }

    public static Path dataName(
            final Path baseDir,
            final int sequence) {
        return baseDir.resolve(sequence + DATA_SUFFIX);
    }

    private static Path tempIndexName(
            final Path baseDir,
            final int sequence) {
        return baseDir.resolve(sequence + INDEX_SUFFIX + TEMP_SUFFIX);
    }

    private static Path tempDataName(
            final Path baseDir,
            final int sequence) {
        return baseDir.resolve(sequence + DATA_SUFFIX + TEMP_SUFFIX);
    }

    public static SSTable dump(
            final Arena arena,
            final Path baseDir,
            final int sequence,
            final Iterator<Entry<MemorySegment>> entries) throws IOException {
        // Write to temporary files
        final Path tempIndexName = tempIndexName(baseDir, sequence);
        final Path tempDataName = tempDataName(baseDir, sequence);

        // Iterate in a single pass
        try (final FileChannel indexChannel =
                     FileChannel.open(
                             tempIndexName,
                             StandardOpenOption.WRITE,
                             StandardOpenOption.CREATE_NEW);
             final FileChannel dataChannel =
                     FileChannel.open(
                             tempDataName,
                             StandardOpenOption.WRITE,
                             StandardOpenOption.CREATE_NEW)) {
            // Will write through memory mapped memory segments
            final MemorySegment index =
                    indexChannel.map(
                            FileChannel.MapMode.READ_WRITE,
                            0L,
                            Long.MAX_VALUE,
                            arena);
            final MemorySegment data =
                    dataChannel.map(
                            FileChannel.MapMode.READ_WRITE,
                            0L,
                            Long.MAX_VALUE,
                            arena);

            long indexOffset = 0L;
            long entryOffset = 0L;

            // Iterate and serialize
            while (entries.hasNext()) {
                // First write offset
                index.set(
                        ValueLayout.OfLong.JAVA_LONG,
                        indexOffset,
                        entryOffset);

                // Then write entry
                final Entry<MemorySegment> entry = entries.next();
                entryOffset = write(entry, data, entryOffset);

                // Advance index
                indexOffset += Long.BYTES;
            }

            // Force using all the facilities
            index.force();
            data.force();
            indexChannel.force(true);
            dataChannel.force(true);
        }

        // Publish files atomically
        // FIRST index, LAST data
        Files.move(
                tempIndexName,
                indexName(baseDir, sequence),
                StandardCopyOption.ATOMIC_MOVE);
        Files.move(
                tempDataName,
                dataName(baseDir, sequence),
                StandardCopyOption.ATOMIC_MOVE);

        // Build and return read-only SSTable
        return new SSTable(arena, baseDir, sequence);
    }

    /**
     * @return {@code offset} advanced
     */
    private static long write(
            final Entry<MemorySegment> entry,
            final MemorySegment to,
            long offset) {
        final MemorySegment key = entry.key();
        final MemorySegment value = entry.value();

        // Key size
        to.set(
                ValueLayout.OfLong.JAVA_LONG,
                offset,
                key.byteSize());
        offset += Long.BYTES;

        // Key
        MemorySegment.copy(
                key,
                0L,
                to,
                offset,
                key.byteSize());
        offset += key.byteSize();

        // Value size and possibly value
        if (value == null) {
            // Tombstone
            to.set(
                    ValueLayout.OfLong.JAVA_LONG,
                    offset,
                    TOMBSTONE_VALUE_LENGTH);
            offset += Long.BYTES;
        } else {
            // Value length
            to.set(
                    ValueLayout.OfLong.JAVA_LONG,
                    offset,
                    value.byteSize());
            offset += Long.BYTES;

            // Value
            MemorySegment.copy(
                    value,
                    0L,
                    to,
                    offset,
                    value.byteSize());
            offset += value.byteSize();
        }

        return offset;
    }
}
