package ru.vk.itmo.danilinandrew;

import ru.vk.itmo.Entry;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.Iterator;

public class DiskStorageWithCompact {
    private final DiskStorage diskStorage;
    private static final String INDEX_FILE = "index.idx";
    private static final String TMP_FILE = "index.tmp";

    private static final String TMP_COMPACTED_FILE = "0tmp";
    private static final String COMPACTED_FILE = "0";

    public DiskStorageWithCompact(DiskStorage diskStorage) {
        this.diskStorage = diskStorage;
    }

    public Iterator<Entry<MemorySegment>> range(
            Iterator<Entry<MemorySegment>> firstIterator,
            MemorySegment from,
            MemorySegment to
    ) {
        return diskStorage.range(firstIterator, from, to);
    }

    public void compact(
            Path storagePath,
            Iterator<Entry<MemorySegment>> it1,
            Iterator<Entry<MemorySegment>> it2
    ) throws IOException {
        long sizeData = 0;
        long sizeIndexes = 0;
        while (it1.hasNext()) {
            Entry<MemorySegment> entry = it1.next();
            if (entry.value() != null) {
                sizeData += entry.key().byteSize();
                sizeData += entry.value().byteSize();
                sizeIndexes++;
            }
        }

        if (sizeIndexes == 0) {
            return;
        }
        sizeIndexes *= 2 * Long.BYTES;

        final Path indexTmp = storagePath.resolve(TMP_FILE);
        final Path indexFile = storagePath.resolve(INDEX_FILE);

        try (
                FileChannel fileChannel = FileChannel.open(
                        storagePath.resolve(TMP_COMPACTED_FILE),
                        StandardOpenOption.WRITE,
                        StandardOpenOption.READ,
                        StandardOpenOption.CREATE,
                        StandardOpenOption.TRUNCATE_EXISTING
                );
                Arena writeArena = Arena.ofConfined()
        ) {
            MemorySegment fileSegment = fileChannel.map(
                    FileChannel.MapMode.READ_WRITE,
                    0,
                    sizeIndexes + sizeData,
                    writeArena
            );
            long offsetIndexes = 0;
            long currentIndex = sizeIndexes;
            while (it2.hasNext()) {
                Entry<MemorySegment> currentEntry = it2.next();
                if (currentEntry.value() != null) {
                    fileSegment.set(ValueLayout.JAVA_LONG_UNALIGNED, offsetIndexes, currentIndex);
                    offsetIndexes += Long.BYTES;

                    MemorySegment.copy(
                            currentEntry.key(),
                            0,
                            fileSegment,
                            currentIndex,
                            currentEntry.key().byteSize()
                    );
                    currentIndex += currentEntry.key().byteSize();

                    fileSegment.set(ValueLayout.JAVA_LONG_UNALIGNED, offsetIndexes, currentIndex);
                    offsetIndexes += Long.BYTES;

                    MemorySegment.copy(
                            currentEntry.value(),
                            0,
                            fileSegment,
                            currentIndex,
                            currentEntry.value().byteSize()
                    );
                    currentIndex += currentEntry.value().byteSize();
                }
            }

            Files.writeString(
                    indexTmp,
                    COMPACTED_FILE,
                    StandardOpenOption.WRITE,
                    StandardOpenOption.CREATE,
                    StandardOpenOption.TRUNCATE_EXISTING
            );

            diskStorage.clearStorage(storagePath);

            Files.move(indexTmp, indexFile, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
            Files.move(
                    storagePath.resolve(TMP_COMPACTED_FILE),
                    storagePath.resolve(COMPACTED_FILE),
                    StandardCopyOption.ATOMIC_MOVE,
                    StandardCopyOption.REPLACE_EXISTING
            );
        }
    }
}
