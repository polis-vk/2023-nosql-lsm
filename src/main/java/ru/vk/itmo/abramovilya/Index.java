package ru.vk.itmo.abramovilya;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

class Index implements Closeable {
    final int number;
    private final MemorySegment mappedIndexFile;
    private long offset;

    private final FileChannel fileChannel;
    private final Path storagePath;
    private final Arena arena = Arena.ofShared();
    private final String sstableFileBaseName;

    private final Arena outerArena;

    Index(int number, long offset, String indexFileBaseName, String sstableFileBaseName, Path storagePath, Arena outerArena) {
        this.number = number;
        this.storagePath = storagePath;
        this.sstableFileBaseName = sstableFileBaseName;
        this.outerArena = outerArena;
        this.offset = offset;


        try {
            Path filePath = storagePath.resolve(indexFileBaseName + number);
            fileChannel = FileChannel.open(filePath);
            mappedIndexFile = fileChannel.map(FileChannel.MapMode.READ_ONLY, 0, Files.size(filePath), arena);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    MemorySegment getValueFromStorage() {
        long inStorageOffset = mappedIndexFile.get(ValueLayout.JAVA_LONG_UNALIGNED, offset);

        Path sstableFilePath = storagePath.resolve(sstableFileBaseName + number);
        try (FileChannel fc = FileChannel.open(sstableFilePath, StandardOpenOption.READ)) {
            MemorySegment mapped = fc.map(FileChannel.MapMode.READ_ONLY, 0, Files.size(sstableFilePath), outerArena);
            long keySize = mapped.get(ValueLayout.JAVA_LONG_UNALIGNED, inStorageOffset);
            inStorageOffset += Long.BYTES;
            inStorageOffset += keySize;

            long valueSize = mapped.get(ValueLayout.JAVA_LONG_UNALIGNED, inStorageOffset);
            if (valueSize == -1) {
                return null;
            }
            inStorageOffset += Long.BYTES;
            return mapped.asSlice(inStorageOffset, valueSize);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }


    MemorySegment nextKey() {
        offset += Long.BYTES;
        long msSize = mappedIndexFile.get(ValueLayout.JAVA_LONG_UNALIGNED, offset);
        offset += Long.BYTES;
        offset += msSize;

        if (offset >= mappedIndexFile.byteSize()) {
            return null;
        }
        msSize = mappedIndexFile.get(ValueLayout.JAVA_LONG_UNALIGNED, offset + Long.BYTES);
        return mappedIndexFile.asSlice(offset + 2 * Long.BYTES, msSize);
    }

    @Override
    public void close() {
        if (fileChannel.isOpen()) {
            try {
                fileChannel.close();
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
        if (arena.scope().isAlive()) {
            arena.close();

        }
    }
}
