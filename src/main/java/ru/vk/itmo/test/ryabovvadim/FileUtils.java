package ru.vk.itmo.test.ryabovvadim;

import ru.vk.itmo.Entry;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;

import static java.lang.foreign.ValueLayout.JAVA_BYTE;

public class FileUtils {
    public static final String DATA_FILE_EXT = "data";
    public static final String OFFSETS_FILE_EXT = "offsets";

    public static final Comparator<? super MemorySegment> MEMORY_SEGMENT_COMPARATOR = (firstSegment, secondSegment) -> {
        if (firstSegment == null) {
            return secondSegment == null ? 0 : -1;
        } else if (secondSegment == null) {
            return 1;
        }

        long firstSegmentSize = firstSegment.byteSize();
        long secondSegmentSize = secondSegment.byteSize();
        long mismatchOffset = firstSegment.mismatch(secondSegment);

        if (mismatchOffset == firstSegmentSize) {
            return -1;
        }
        if (mismatchOffset == secondSegmentSize) {
            return 1;
        }
        if (mismatchOffset == -1) {
            return Long.compare(firstSegmentSize, secondSegmentSize);
        }

        return Byte.compare(firstSegment.get(JAVA_BYTE, mismatchOffset), secondSegment.get(JAVA_BYTE, mismatchOffset));
    };

    public static final Comparator<? super Entry<MemorySegment>> ENTRY_COMPARATOR =
        Comparator.comparing(Entry::key, MEMORY_SEGMENT_COMPARATOR);

    public static Path makePath(Path prefix, String name, String extension) {
        return Path.of(prefix.toString(), name + "." + extension);
    }
    
    public static void createParentDirectories(Path path) throws IOException {
        if (path == null || path.getParent() == null) {
            return;
        }
        
        Path parent = path.getParent();
        if (Files.notExists(parent)) {
            Files.createDirectories(parent);
        }
    }
    
    private FileUtils() {}
}
