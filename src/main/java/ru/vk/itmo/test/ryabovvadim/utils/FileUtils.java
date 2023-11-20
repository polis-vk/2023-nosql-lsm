package ru.vk.itmo.test.ryabovvadim.utils;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

public final class FileUtils {
    public static final String DATA_FILE_EXT = "data";
    public static final String OFFSETS_FILE_EXT = "offsets";

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
