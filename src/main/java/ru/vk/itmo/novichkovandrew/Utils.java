package ru.vk.itmo.novichkovandrew;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public final class Utils {
    private Utils() {
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
    public static long writeLong(FileChannel channel, long offset, long value) throws IOException {
        channel.position(offset);
        ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
        buffer.putLong(value);
        buffer.flip();
        int bytes = channel.write(buffer);
        return offset + bytes;
    }

    /**
     * Read long value from file opened in FileChannel.
     */
    public static long readLong(FileChannel channel, long offset) throws IOException {
        channel.position(offset);
        ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
        channel.read(buffer);
        buffer.flip();
        return buffer.getLong();
    }
}
