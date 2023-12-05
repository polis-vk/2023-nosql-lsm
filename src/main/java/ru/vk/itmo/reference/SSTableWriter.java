package ru.vk.itmo.reference;

import ru.vk.itmo.Entry;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.Iterator;

/**
 * Writes {@link Entry} {@link Iterator} to SSTable on disk.
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
final class SSTableWriter {
    // Reusable buffers to eliminate allocations.
    // But excessive memory copying is still there :(
    // Long cell
    private final SegmentBuffer longBuffer = new SegmentBuffer(Long.BYTES);
    // Growable blob cell
    private final SegmentBuffer blobBuffer = new SegmentBuffer(512);

    void write(
            final Path baseDir,
            final int sequence,
            final Iterator<Entry<MemorySegment>> entries) throws IOException {
        // Write to temporary files
        final Path tempIndexName = SSTables.tempIndexName(baseDir, sequence);
        final Path tempDataName = SSTables.tempDataName(baseDir, sequence);

        // Iterate in a single pass!
        // Will write through FileChannel despite extra memory copying and
        // no buffering (which may be implemented later).
        // Looking forward to MemorySegment facilities in FileChannel!
        try (FileChannel index =
                     FileChannel.open(
                             tempIndexName,
                             StandardOpenOption.READ,
                             StandardOpenOption.WRITE,
                             StandardOpenOption.CREATE,
                             StandardOpenOption.TRUNCATE_EXISTING);
             FileChannel data =
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
                writeLong(
                        entryOffset,
                        index,
                        indexOffset);

                // Then write the entry
                final Entry<MemorySegment> entry = entries.next();
                entryOffset =
                        writeEntry(
                                entry,
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
                SSTables.indexName(
                        baseDir,
                        sequence);
        Files.move(
                tempIndexName,
                indexName,
                StandardCopyOption.ATOMIC_MOVE,
                StandardCopyOption.REPLACE_EXISTING);
        final Path dataName =
                SSTables.dataName(
                        baseDir,
                        sequence);
        Files.move(
                tempDataName,
                dataName,
                StandardCopyOption.ATOMIC_MOVE,
                StandardCopyOption.REPLACE_EXISTING);
    }

    static void writeFully(
            final FileChannel channel,
            final ByteBuffer buffer,
            final long offset) throws IOException {
        long position = offset;
        while (buffer.hasRemaining()) {
            position += channel.write(buffer, position);
        }
    }

    private void writeLong(
            final long value,
            final FileChannel channel,
            final long offset) throws IOException {
        longBuffer.segment().set(
                ValueLayout.OfLong.JAVA_LONG_UNALIGNED,
                0,
                value);
        writeFully(
                channel,
                longBuffer.buffer(),
                offset);
    }

    private void writeSegment(
            final MemorySegment value,
            final FileChannel channel,
            final long offset) throws IOException {
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
    }

    /**
     * Writes {@link Entry} to {@link FileChannel}.
     *
     * @return {@code offset} advanced
     */
    private long writeEntry(
            final Entry<MemorySegment> entry,
            final FileChannel channel,
            final long from) throws IOException {
        final MemorySegment key = entry.key();
        final MemorySegment value = entry.value();
        long offset = from;

        // Key size
        writeLong(
                key.byteSize(),
                channel,
                offset);
        offset += Long.BYTES;

        // Key
        writeSegment(
                key,
                channel,
                offset);
        offset += key.byteSize();

        // Value size and possibly value
        if (value == null) {
            // Tombstone
            writeLong(
                    SSTables.TOMBSTONE_VALUE_LENGTH,
                    channel,
                    offset);
            offset += Long.BYTES;
        } else {
            // Value length
            writeLong(
                    value.byteSize(),
                    channel,
                    offset);
            offset += Long.BYTES;

            // Value
            writeSegment(
                    value,
                    channel,
                    offset);
            offset += value.byteSize();
        }

        return offset;
    }
}
