package ru.vk.itmo.novichkovandrew;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Stream;

public class Utils {

    /**
     * Executes amount of files by path.
     * Stream does not create an explicit list of files
     * but simply counts the number of files in the directory.
     */
    public static long filesCount(Path path) {
        try (Stream<Path> files = Files.list(path)) {
            return files.count();
        } catch (IOException ex) {
            System.err.printf("Couldn't calc amount of files in directory %s: %s", path, ex.getMessage());
        }
        return -1;
    }

    /**
     * Copy from one MemorySegment to another and return new offset of two segments.
     */
    public static long copyToSegment(MemorySegment to, MemorySegment from, long offset) {
        MemorySegment.copy(from, 0, to, offset, from.byteSize());
        return offset + from.byteSize();
    }

    /**
     * Cast object value to string, and wrap its byte array into MemorySegment.
     */
    public static MemorySegment toMemorySegment(Object value) {
        return MemorySegment.ofArray(value.toString().getBytes(StandardCharsets.UTF_8));
    }
}
