package ru.vk.itmo.test.ryabovvadim.utils;

import ru.vk.itmo.Entry;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;

import static java.lang.foreign.ValueLayout.JAVA_BYTE;

public final class FileUtils {
    public static final String DATA_FILE_EXT = "data";
    public static final String OFFSETS_FILE_EXT = "offsets";

    public static final Comparator<? super Entry<MemorySegment>> ENTRY_COMPARATOR =
        Comparator.comparing(Entry::key, FileUtils::compareMemorySegments);
    
    public static int compareMemorySegments(MemorySegment l, MemorySegment r) {
        return compareMemorySegments(l, 0, l.byteSize(), r, 0, r.byteSize());
    }
    
    public static int compareMemorySegments(
        MemorySegment l,
        long lFromOffset,
        long lToOffset,
        MemorySegment r,
        long rFromOffset,
        long rToOffset
    ) {
        if (l == null) {
            return r == null ? 0 : -1;
        } else if (r == null) {
            return 1;
        }

        long lSize = lToOffset - lFromOffset;
        long rSize = rToOffset - rFromOffset;
        long mismatch = MemorySegment.mismatch(l, lFromOffset, lToOffset, r, rFromOffset, rToOffset);

        if (mismatch == lSize) {
            return -1;
        }
        if (mismatch == rSize) {
            return 1;
        }
        if (mismatch == -1) {
            return 0;
        }

        return Byte.compareUnsigned(
            l.get(JAVA_BYTE, lFromOffset + mismatch), 
            r.get(JAVA_BYTE, rFromOffset + mismatch)
        );
    }

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
    
    private FileUtils() {
    }
}
