package ru.vk.itmo.chernyshevyaroslav;

import ru.vk.itmo.Entry;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
//import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class CompactorUtils {

    CompactorUtils() {
        throw new IllegalStateException("Utility class");
    }

    public static void compact(Path storagePath) throws IOException {
        final Path indexFile = storagePath.resolve(DiskStorage.INDEX_IDX);

        try {
            Files.createFile(indexFile);
        } catch (FileAlreadyExistsException ignored) {
            // it is ok, actually it is normal state
        }
        List<String> existingFiles = Files.readAllLines(indexFile, StandardCharsets.UTF_8);

        Arena arena = Arena.ofConfined();

        NavigableMap<MemorySegment, Entry<MemorySegment>> localStorage =
                new ConcurrentSkipListMap<>(InMemoryDao::compare);

        List<Iterator<Entry<MemorySegment>>> iterators = DiskStorage.loadOrRecover(storagePath, arena).stream()
                .map(it -> DiskStorage.iterator(it, null, null)).toList();

        List<MemorySegment> load = DiskStorage.loadOrRecover(storagePath, arena);

        if (load.isEmpty()) {
            return;
        }

        //List<Iterator<Entry<MemorySegment>>> iteratorsL =
        //        load.stream().limit(2).map(it -> DiskStorage.iterator(it, null, null)).toList();

        //Files.find()


        for (Iterator<Entry<MemorySegment>> iterator : iterators) {
            while (iterator.hasNext()) {
                Entry<MemorySegment> entry = iterator.next();
                if (entry.value() == null) {
                    localStorage.remove(entry.key());
                } else {
                    localStorage.put(entry.key(), entry);
                }
            }
        }

        for (String file : existingFiles) {
            Files.delete(storagePath.resolve(file));
        }

        DiskStorage.save(storagePath, localStorage.values());

        Files.writeString(
                indexFile,
                "0",
                StandardOpenOption.WRITE,
                StandardOpenOption.CREATE,
                StandardOpenOption.TRUNCATE_EXISTING
        );

        Files.move(storagePath.resolve(String.valueOf(existingFiles.size())),
                storagePath.resolve("0"),
                StandardCopyOption.ATOMIC_MOVE,
                StandardCopyOption.REPLACE_EXISTING);
    }

}
