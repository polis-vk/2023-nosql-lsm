package ru.vk.itmo.abramovilya;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

class SSTable implements Closeable, Table {
    private final int number;
    private final MemorySegment mappedIndexFile;
    private final MemorySegment mappedStorageFile;
    private long offset;
    private final FileChannel indexFileChannel;
    private final FileChannel storageFileChannel;
    private final Path storagePath;
    private final Arena arena = Arena.ofShared();
    private final String sstableFileBaseName;
    private MemorySegment currentKey;

    private final Arena outerArena;

    SSTable(int number, long offset, String indexFileBaseName, String sstableFileBaseName, Path storagePath, Arena outerArena) {
        this.number = number;
        this.storagePath = storagePath;
        this.sstableFileBaseName = sstableFileBaseName;
        this.outerArena = outerArena;
        this.offset = offset;

        try {
            Path indexFilePath = storagePath.resolve(indexFileBaseName + number);
            indexFileChannel = FileChannel.open(indexFilePath);
            mappedIndexFile = indexFileChannel.map(FileChannel.MapMode.READ_ONLY, 0, Files.size(indexFilePath), arena);

            Path storageFilePath = storagePath.resolve(sstableFileBaseName + number);
            storageFileChannel = FileChannel.open(storageFilePath);
            mappedStorageFile = storageFileChannel.map(FileChannel.MapMode.READ_ONLY, 0, Files.size(storageFilePath), arena);

            long storageOffset = mappedIndexFile.get(ValueLayout.JAVA_LONG_UNALIGNED, offset);
            long msSize = mappedStorageFile.get(ValueLayout.JAVA_LONG_UNALIGNED, storageOffset);
            currentKey = mappedStorageFile.asSlice(storageOffset + Long.BYTES, msSize);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public MemorySegment getKey() {
        return currentKey;
    }

    @Override
    public MemorySegment getValue() {
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


    @Override
    public MemorySegment nextKey() {
        offset += 2 * Long.BYTES;
        if (offset >= mappedIndexFile.byteSize()) {
            return null;
        }
        long storageOffset = mappedIndexFile.get(ValueLayout.JAVA_LONG_UNALIGNED, offset);

        long msSize = mappedStorageFile.get(ValueLayout.JAVA_LONG_UNALIGNED, storageOffset);
        storageOffset += Long.BYTES;
        MemorySegment key = mappedStorageFile.asSlice(storageOffset, msSize);
        currentKey = key;
        return key;
    }

    @Override
    public void close() {
        if (indexFileChannel.isOpen()) {
            try {
                indexFileChannel.close();
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
        if (arena.scope().isAlive()) {
            arena.close();

        }
    }

    @Override
    public int number() {
        return number;
    }
}
