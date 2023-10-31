package ru.vk.itmo.novichkovandrew;

import ru.vk.itmo.BaseEntry;
import ru.vk.itmo.Entry;
import ru.vk.itmo.novichkovandrew.exceptions.ReadFailureException;
import ru.vk.itmo.novichkovandrew.exceptions.WriteFailureException;
import sun.misc.Unsafe;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Stream;

public final class Utils {
    /**
     * Constant for meta block size.
     * Now equals long byte size * 2 -> two pointers: for indexBlock and for dataBlock
     * Filter Block will add in the future, so this size will be 3 * long bsz.
     */
    public static final long META_BLOCK_SIZE = 2L * Long.BYTES; //todo move to constants
    public static final long PAGE_SIZE = pageSize();
    public static final Entry<MemorySegment> EMPTY = new BaseEntry<>(MemorySegment.NULL, MemorySegment.NULL);
    public static final long FOOTER_SIZE = 2*Long.BYTES;


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


    /**
     * Return index length of SSTable file.
     * Metadata contains amount of entries in sst, offsets and size of keys.
     * It has the following format: <var>keyOff1:valOff1 keyOff2:valOff2 ...
     * keyOff_n:valOff_n keyOff_n+1:valOff_n+1</var>
     * without any : and spaces.
     */

    public static long indexByteSize(long size) {
        return 2L * (size + 1) * Long.BYTES;
    }

    private static long pageSize() {
        try {
            Field field = Unsafe.class.getDeclaredField("theUnsafe");
            field.setAccessible(true);
            Unsafe unsafe = (Unsafe) field.get(null);
            return unsafe.pageSize();
        } catch (Exception ex) { // TODO: if Java version < 9, return 4 kilobytes, fix for old java.
            return 4L * 1024;
        }
    }

    public static Path sstTablePath(Path path, long suffix) {
        String fileName = String.format("data-%s.txt", suffix);
        return path.resolve(Path.of(fileName));
    }
    public Path sstIndexPath(Path path, long suffix) {
        String fileName = String.format("index-%s.txt", suffix);
        return path.resolve(Path.of(fileName));
    }


}
