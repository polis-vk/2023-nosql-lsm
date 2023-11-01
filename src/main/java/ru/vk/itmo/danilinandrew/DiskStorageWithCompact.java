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
    DiskStorage diskStorage;
    private static final String INDEX_FILE = "index.idx";
    private static final String TMP_FILE = "index.tmp";

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

        final Path indexFile = storagePath.resolve(INDEX_FILE);
        final String fileNameTmp = "tmpFile";

        try (
                FileChannel fileChannel = FileChannel.open(
                        storagePath.resolve(fileNameTmp),
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

                    fileSegment.set(ValueLayout.JAVA_LONG_UNALIGNED,  offsetIndexes, currentIndex);
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

            diskStorage.clearStorage(storagePath);

            final String dataFileName = "0";

            Files.move(
                    indexFile,
                    storagePath.resolve(TMP_FILE),
                    StandardCopyOption.ATOMIC_MOVE,
                    StandardCopyOption.REPLACE_EXISTING
            );
            Files.writeString(
                    storagePath.resolve(INDEX_FILE),
                    dataFileName,
                    StandardOpenOption.WRITE,
                    StandardOpenOption.CREATE,
                    StandardOpenOption.TRUNCATE_EXISTING
            );
            Files.move(
                    storagePath.resolve(fileNameTmp),
                    storagePath.resolve(dataFileName),
                    StandardCopyOption.ATOMIC_MOVE,
                    StandardCopyOption.REPLACE_EXISTING
            );
            Files.delete(storagePath.resolve(TMP_FILE));
        }
    }
}
