package ru.vk.itmo.tveritinalexandr;

import ru.vk.itmo.Entry;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static ru.vk.itmo.tveritinalexandr.Utils.tombstone;

public class Compactor {

    private Compactor() {
    }

    // В методе есть много схожей логики с методом DiskStorage.save(),
    //но решил нарушить DRY, чтобы вам было удобнее проверять т.к. я взял референс 3-ого этапа
    //и скорее всего весь PR будет зелёным)
    public static void compactAndSave(Path storagePath, DiskStorage diskStorage,
                                      Iterator<Entry<MemorySegment>> inMemoryIterator)
            throws IOException {
        final Path indexTmp = storagePath.resolve("index.tmp");
        final Path indexFile = storagePath.resolve("index.idx");
        final String compactingFileName = "compacting";

        try {
            Files.createFile(indexFile);
        } catch (FileAlreadyExistsException ignored) {
            // it is ok, actually it is normal state
        }
        List<String> existedFiles = Files.readAllLines(indexFile, StandardCharsets.UTF_8);

        // index_size:
        long dataSize = 0;
        long count = 0;
        var firstCycle = diskStorage.range(inMemoryIterator, null, null);
        while (firstCycle.hasNext()) {
            var entry = firstCycle.next();
            dataSize += entry.key().byteSize();
            MemorySegment value = entry.value();
            if (value != null) {
                dataSize += value.byteSize();
            }
            count++;
        }

        long indexSize = count * 2 * Long.BYTES;

        try (
                FileChannel fileChannel = FileChannel.open(
                        storagePath.resolve(compactingFileName),
                        StandardOpenOption.WRITE,
                        StandardOpenOption.READ,
                        StandardOpenOption.CREATE
                );
                Arena writeArena = Arena.ofConfined()
        ) {
            MemorySegment fileSegment = fileChannel.map(
                    FileChannel.MapMode.READ_WRITE,
                    0,
                    indexSize + dataSize,
                    writeArena
            );

            // Прыгаем по страницам, но в 1 проход по итератору
            // index_and_data:
            long dataOffset = indexSize;
            int indexOffset = 0;
            var secondCycle = diskStorage.range(inMemoryIterator, null, null);
            while (secondCycle.hasNext()) {
                var entry = secondCycle.next();
                MemorySegment key = entry.key();
                fileSegment.set(ValueLayout.JAVA_LONG_UNALIGNED, indexOffset, dataOffset);
                MemorySegment.copy(key, 0, fileSegment, dataOffset, key.byteSize());

                dataOffset += key.byteSize();
                indexOffset += Long.BYTES;

                MemorySegment value = entry.value();
                if (value == null) {
                    fileSegment.set(ValueLayout.JAVA_LONG_UNALIGNED, indexOffset, tombstone(dataOffset));
                } else {
                    MemorySegment.copy(value, 0, fileSegment, dataOffset, value.byteSize());
                    fileSegment.set(ValueLayout.JAVA_LONG_UNALIGNED, indexOffset, dataOffset);
                    dataOffset += value.byteSize();
                }
                indexOffset += Long.BYTES;
            }
        }

        Files.move(indexFile, indexTmp, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);

        List<String> list = new ArrayList<>(existedFiles.size() + 1);
        list.addAll(existedFiles);
        list.add(compactingFileName);
        Files.write(
                indexFile,
                list,
                StandardOpenOption.WRITE,
                StandardOpenOption.CREATE,
                StandardOpenOption.TRUNCATE_EXISTING
        );
        Files.delete(indexTmp);
    }
}
