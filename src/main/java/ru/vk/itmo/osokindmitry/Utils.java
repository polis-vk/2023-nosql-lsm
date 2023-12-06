package ru.vk.itmo.osokindmitry;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;

public final class Utils {
    private static final String INDEX_FILE_NAME = "index";
    private static final String SSTABLE_EXT = ".sstable";
    private static final String TMP_EXT = ".tmp";

    private Utils() {
        throw new UnsupportedOperationException("This is a utility class and cannot be instantiated");
    }

    public static List<Path> loadOrRecover(Path storagePath) throws IOException {
        final Path indexTmp = storagePath.resolve(INDEX_FILE_NAME + TMP_EXT);
        final Path indexFile = storagePath.resolve(INDEX_FILE_NAME + SSTABLE_EXT);

        if (Files.exists(indexTmp)) {
            Files.move(indexTmp, indexFile, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
        } else {
            try {
                Files.createFile(indexFile);
            } catch (FileAlreadyExistsException ignored) {
                // it is ok, actually it is normal state
            }
        }

        List<String> existedFiles = Files.readAllLines(indexFile, StandardCharsets.UTF_8);
        List<Path> result = new ArrayList<>(existedFiles.size());
        for (String fileName : existedFiles) {
            Path file = storagePath.resolve(fileName);
            result.add(file);
        }
        return result;
    }

}
