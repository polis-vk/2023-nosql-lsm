package ru.vk.itmo.reference;

import ru.vk.itmo.Entry;

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
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Stream;

import static ru.vk.itmo.reference.FileUtils.mapReadOnly;
import static ru.vk.itmo.reference.FileUtils.writeFully;

/**
 * Provides {@link SSTable} management facilities: dumping and discovery.
 *
 * <p>Index file {@code <N>.index} contains {@code long} offsets to entries in data file:
 * {@code [offset0, offset1, ...]}
 *
 * <p>Data file {@code <N>.data} contains serialized entries:
 * {@code <long keyLength><key><long valueLength><value>}
 *
 * <p>Tombstones are encoded as {@code valueLength} {@code -1} and no subsequent value.
 *
 * @author incubos
 */
final class SSTables {
    public static final String INDEX_SUFFIX = ".index";
    public static final String DATA_SUFFIX = ".data";
    public static final long TOMBSTONE_VALUE_LENGTH = -1L;

    private static final String TEMP_SUFFIX = ".tmp";

    /**
     * Can't instantiate.
     */
    private SSTables() {
        // Only static methods
    }

    private static Path indexName(
            final Path baseDir,
            final int sequence) {
        return baseDir.resolve(sequence + INDEX_SUFFIX);
    }

    private static Path dataName(
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

    /**
     * Returns {@link List} of {@link SSTable}s from <b>freshest</b> to oldest.
     */
    static List<SSTable> discover(
            final Arena arena,
            final Path baseDir) throws IOException {
        if (!Files.exists(baseDir)) {
            return Collections.emptyList();
        }

        final List<SSTable> result = new ArrayList<>();
        try (final Stream<Path> files = Files.list(baseDir)) {
            files.forEach(file -> {
                if (!file.getFileName().toString().endsWith(DATA_SUFFIX)) {
                    // Skip non data
                    return;
                }

                final String fileName = file.getFileName().toString();
                final int sequence =
                        // <N>.data -> N
                        Integer.parseInt(
                                fileName.substring(
                                        0,
                                        fileName.length() - DATA_SUFFIX.length()));

                try {
                    result.add(open(arena, baseDir, sequence));
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            });
        }

        // Sort from freshest to oldest
        result.sort((o1, o2) -> Integer.compare(o2.sequence, o1.sequence));

        return Collections.unmodifiableList(result);
    }

    static SSTable open(
            final Arena arena,
            final Path baseDir,
            final int sequence) throws IOException {
        final MemorySegment index =
                mapReadOnly(
                        arena,
                        indexName(baseDir, sequence));
        final MemorySegment data =
                mapReadOnly(
                        arena,
                        dataName(baseDir, sequence));

        return new SSTable(
                sequence,
                index,
                data);
    }

    static void remove(
            final Path baseDir,
            final int sequence) throws IOException {
        // First delete data file to make SSTable invisible
        Files.delete(dataName(baseDir, sequence));
        Files.delete(indexName(baseDir, sequence));
    }

    static void promote(
            final Path baseDir,
            final int from,
            final int to) throws IOException {
        // Build to progress to the same outcome
        if (Files.exists(indexName(baseDir, from))) {
            Files.move(
                    indexName(baseDir, from),
                    indexName(baseDir, to),
                    StandardCopyOption.ATOMIC_MOVE,
                    StandardCopyOption.REPLACE_EXISTING);
        }
        if (Files.exists(dataName(baseDir, from))) {
            Files.move(
                    dataName(baseDir, from),
                    dataName(baseDir, to),
                    StandardCopyOption.ATOMIC_MOVE,
                    StandardCopyOption.REPLACE_EXISTING);
        }
    }

    static void write(
            final Path baseDir,
            final int sequence,
            final Iterator<Entry<MemorySegment>> entries) throws IOException {
        // Write to temporary files
        final Path tempIndexName = tempIndexName(baseDir, sequence);
        final Path tempDataName = tempDataName(baseDir, sequence);

        // Reusable buffers to eliminate allocations.
        // But excessive memory copying is still there :(

        // Long cell
        final SegmentBuffer longBuffer = new SegmentBuffer(Long.BYTES);

        // Growable blob cell
        final SegmentBuffer blobBuffer = new SegmentBuffer(512);

        // Iterate in a single pass!
        // Will write through FileChannel despite extra memory copying and
        // no buffering (which may be implemented later).
        // Looking forward to MemorySegment facilities in FileChannel!
        try (final FileChannel index =
                     FileChannel.open(
                             tempIndexName,
                             StandardOpenOption.READ,
                             StandardOpenOption.WRITE,
                             StandardOpenOption.CREATE,
                             StandardOpenOption.TRUNCATE_EXISTING);
             final FileChannel data =
                     FileChannel.open(
                             tempDataName,
                             StandardOpenOption.READ,
                             StandardOpenOption.WRITE,
                             StandardOpenOption.CREATE,
                             StandardOpenOption.TRUNCATE_EXISTING)) {
            long indexOffset = 0L;
            long entryOffset = 0L;

            // Iterate and serialize
            while (entries.hasNext()) {
                // First write offset to the entry
                longBuffer.segment().set(
                        ValueLayout.OfLong.JAVA_LONG_UNALIGNED,
                        0,
                        entryOffset);
                writeFully(
                        index,
                        longBuffer.buffer(),
                        indexOffset);

                // Then write the entry
                final Entry<MemorySegment> entry = entries.next();
                entryOffset =
                        write(
                                entry,
                                longBuffer,
                                blobBuffer,
                                data,
                                entryOffset);

                // Advance index
                indexOffset += Long.BYTES;
            }

            // Force using all the facilities
            index.force(true);
            data.force(true);
        }

        // Publish files atomically
        // FIRST index, LAST data
        final Path indexName =
                indexName(
                        baseDir,
                        sequence);
        Files.move(
                tempIndexName,
                indexName,
                StandardCopyOption.ATOMIC_MOVE,
                StandardCopyOption.REPLACE_EXISTING);
        final Path dataName =
                dataName(
                        baseDir,
                        sequence);
        Files.move(
                tempDataName,
                dataName,
                StandardCopyOption.ATOMIC_MOVE,
                StandardCopyOption.REPLACE_EXISTING);
    }

    /**
     * Writes {@link Entry} to {@link FileChannel} reusing supplied buffers.
     *
     * @return {@code offset} advanced
     */
    private static long write(
            final Entry<MemorySegment> entry,
            final SegmentBuffer longBuffer,
            final SegmentBuffer blobBuffer,
            final FileChannel channel,
            long offset) throws IOException {
        final MemorySegment key = entry.key();
        final MemorySegment value = entry.value();

        // Key size
        longBuffer.segment().set(
                ValueLayout.OfLong.JAVA_LONG_UNALIGNED,
                0,
                key.byteSize());
        writeFully(
                channel,
                longBuffer.buffer(),
                offset);
        offset += Long.BYTES;

        // Key
        blobBuffer.limit(key.byteSize());
        MemorySegment.copy(
                key,
                0L,
                blobBuffer.segment(),
                0L,
                key.byteSize());
        writeFully(
                channel,
                blobBuffer.buffer(),
                offset);
        offset += key.byteSize();

        // Value size and possibly value
        if (value == null) {
            // Tombstone
            longBuffer.segment().set(
                    ValueLayout.OfLong.JAVA_LONG_UNALIGNED,
                    0,
                    TOMBSTONE_VALUE_LENGTH);
            writeFully(
                    channel,
                    longBuffer.buffer(),
                    offset);
            offset += Long.BYTES;
        } else {
            // Value length
            longBuffer.segment().set(
                    ValueLayout.OfLong.JAVA_LONG_UNALIGNED,
                    0,
                    value.byteSize());
            writeFully(
                    channel,
                    longBuffer.buffer(),
                    offset);
            offset += Long.BYTES;

            // Value
            blobBuffer.limit(value.byteSize());
            MemorySegment.copy(
                    value,
                    0L,
                    blobBuffer.segment(),
                    0L,
                    value.byteSize());
            writeFully(
                    channel,
                    blobBuffer.buffer(),
                    offset);
            offset += value.byteSize();
        }

        return offset;
    }
}
