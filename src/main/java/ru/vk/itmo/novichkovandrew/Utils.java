package ru.vk.itmo.novichkovandrew;

import ru.vk.itmo.novichkovandrew.exceptions.ReadFailureException;
import ru.vk.itmo.novichkovandrew.exceptions.WriteFailureException;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Stream;

public final class Utils {
    public static final MemorySegment LEFT = MemorySegment.NULL;
    public static final MemorySegment RIGHT = MemorySegment.NULL;

    /**
     * No instances.
     */
    private Utils() {
    }

    /**
     * Executes amount of files by path.
     * Stream does not create an explicit list of files
     * but simply counts the number of files in the directory.
     */
    public static int filesCount(Path path) {
        try (Stream<Path> files = Files.list(path)) {
            return Math.toIntExact(files.count());
        } catch (IOException ex) {
            return 0;
        }
    }

    /**
     * Copy from one MemorySegment to another and return new offset of two segments.
     */
    public static long copyToSegment(MemorySegment to, MemorySegment from, long offset) {
        MemorySegment source = from == null ? MemorySegment.NULL : from;
        MemorySegment.copy(source, 0, to, offset, source.byteSize());
        return offset + source.byteSize();
    }

    /**
     * Writes long value into file opened in FileChannel.
     * Returns new offset of fileChannel.
     */
    public static long writeLong(FileChannel channel, long offset, long value) {
        try {
            channel.position(offset);
            ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
            buffer.putLong(value);
            buffer.flip();
            int bytes = channel.write(buffer);
            return offset + bytes;
        } catch (IOException ex) {
            throw new WriteFailureException("Failed to write long" + value + "value from position" + offset, ex);
        }
    }

    /**
     * Read long value from file opened in FileChannel.
     */
    public static long readLong(FileChannel channel, long offset) {
        try {
            channel.position(offset);
            ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
            channel.read(buffer);
            buffer.flip();
            return buffer.getLong();
        } catch (IOException ex) {
            throw new ReadFailureException("Failed to write long value from position" + offset, ex);
        }
    }
}
