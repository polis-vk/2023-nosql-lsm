package ru.vk.itmo.novichkovandrew.utils;

import ru.vk.itmo.BaseEntry;
import ru.vk.itmo.Entry;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Stream;

public final class Utils {

    public static final Entry<MemorySegment> EMPTY = new BaseEntry<>(MemorySegment.NULL, MemorySegment.NULL);

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
     * Return index length of SSTable file.
     * Metadata contains amount of entries in sst, offsets and size of keys.
     * It has the following format: <var>keyOff1:valOff1 keyOff2:valOff2 ...
     * keyOff_n:valOff_n keyOff_n+1:valOff_n+1</var>
     * without any : and spaces.
     */
    public static long indexByteSize(long size) {
        return 2L * (size + 1) * Long.BYTES;
    }

    public static Path sstTablePath(Path path, long suffix) {
        String fileName = String.format("data-%s.txt", suffix);
        return path.resolve(Path.of(fileName));
    }
}
