package ru.vk.itmo.chernyshevyaroslav;

import ru.vk.itmo.Entry;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.List;

public class CompactorUtils {

    CompactorUtils() {
        throw new IllegalStateException("Utility class");
    }

    public static void compact(Path storagePath, Iterable<Entry<MemorySegment>> iterable) throws IOException {
        final Path indexFile = storagePath.resolve(DiskStorage.INDEX_IDX);

        try {
            Files.createFile(indexFile);
        } catch (FileAlreadyExistsException ignored) {
            // it is ok, actually it is normal state
        }
        List<String> existingFiles = Files.readAllLines(indexFile, StandardCharsets.UTF_8);

        DiskStorage.save(storagePath, iterable, true);

        for (int i = 0; i < existingFiles.size(); i++) {
            String file = existingFiles.get(i);
            Files.delete(storagePath.resolve(file));
        }

        Files.writeString(
                indexFile,
                "0",
                StandardOpenOption.WRITE,
                StandardOpenOption.CREATE,
                StandardOpenOption.TRUNCATE_EXISTING
        );

        Files.move(storagePath.resolve(String.valueOf(existingFiles.size() + 1)),
                storagePath.resolve("0"),
                StandardCopyOption.ATOMIC_MOVE,
                StandardCopyOption.REPLACE_EXISTING);
    }

}
