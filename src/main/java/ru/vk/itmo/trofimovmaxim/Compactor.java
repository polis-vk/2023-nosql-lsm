package ru.vk.itmo.trofimovmaxim;

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
import java.util.Collection;
import java.util.Iterator;
import java.util.stream.Stream;

public final class Compactor {
    public static void compact(DiskStorage diskStorage,
                               Path storagePath, Collection<Entry<MemorySegment>> memTable) throws IOException {
        final Path indexTmp = storagePath.resolve(DiskStorage.INDEX_TMP);
        final Path indexFile = storagePath.resolve(DiskStorage.INDEX_IDX);
        final Path compactPath = storagePath.resolve("compact");
        final Path compactResPath = storagePath.resolve("0");

        Iterator<Entry<MemorySegment>> iter = diskStorage.range(memTable.iterator(), null, null);
        if (!iter.hasNext()) {
            return;
        }

        long dataSize = 0;
        long count = 0;
        while (iter.hasNext()) {
            Entry<MemorySegment> current = iter.next();

            dataSize += current.key().byteSize();
            MemorySegment value = current.value();
            if (value != null) {
                dataSize += value.byteSize();
            }
            count++;
        }
        long indexSize = count * 2 * Long.BYTES;

        try (
                FileChannel fileChannel = FileChannel.open(
                        compactPath,
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

            long dataOffset = indexSize;
            int indexOffset = 0;
            iter = diskStorage.range(memTable.iterator(), null, null);
            while (iter.hasNext()) {
                Entry<MemorySegment> current = iter.next();

                fileSegment.set(ValueLayout.JAVA_LONG_UNALIGNED, indexOffset, dataOffset);
                MemorySegment key = current.key();
                MemorySegment.copy(key, 0, fileSegment, dataOffset, key.byteSize());
                dataOffset += key.byteSize();
                indexOffset += Long.BYTES;

                MemorySegment value = current.value();
                if (value == null) {
                    fileSegment.set(ValueLayout.JAVA_LONG_UNALIGNED, indexOffset, DiskStorage.tombstone(dataOffset));
                } else {
                    fileSegment.set(ValueLayout.JAVA_LONG_UNALIGNED, indexOffset, dataOffset);
                    MemorySegment.copy(value, 0, fileSegment, dataOffset, value.byteSize());
                    dataOffset += value.byteSize();
                }
                indexOffset += Long.BYTES;
            }
        }

        Files.move(indexFile, indexTmp, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
        Files.writeString(
                indexFile,
                "0",
                StandardOpenOption.WRITE,
                StandardOpenOption.CREATE,
                StandardOpenOption.TRUNCATE_EXISTING
        );

        try (Stream<Path> walker = Files.walk(storagePath.toAbsolutePath())) {
            walker.forEach(
                    path -> {
                        if (path.toFile().isFile() && !path.equals(compactPath) && !path.equals(indexFile)) {
                            try {
                                Files.delete(path);
                            } catch (IOException e) {
                                throw new ClearSsTablesException(e);
                            }
                        }
                    }
            );
        }

        Files.move(compactPath, compactResPath, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
    }

    private Compactor() {
    }

    private static class ClearSsTablesException extends RuntimeException {
        public ClearSsTablesException(Throwable cause) {
            super(cause);
        }
    }
}
