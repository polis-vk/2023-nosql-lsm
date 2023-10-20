package ru.vk.itmo.alenkovayulya;

import ru.vk.itmo.BaseEntry;
import ru.vk.itmo.Config;
import ru.vk.itmo.Entry;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;

import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.TRUNCATE_EXISTING;
import static java.nio.file.StandardOpenOption.WRITE;

public class PersistentFileHandler {
    private static final String STORAGE_NAME = "store";
    private static final String STORAGE_INDEXES_NAME = "store_indexes";
    private final Path storagePath;
    private final Path storageIndexesPath;

    public PersistentFileHandler(Config config) {
        this.storagePath = config.basePath().resolve(Path.of(STORAGE_NAME));
        this.storageIndexesPath = config.basePath().resolve(Path.of(STORAGE_INDEXES_NAME));

    }

    public Entry<MemorySegment> readByKey(MemorySegment key) {
        try (var storageChannel = FileChannel.open(storagePath, READ);
             var indexesChannel = FileChannel.open(storageIndexesPath, READ)) {
            var storageSegment = storageChannel.map(
                    FileChannel.MapMode.READ_ONLY, 0, Files.size(storagePath), Arena.global());
            var indexesSegment = indexesChannel.map(
                    FileChannel.MapMode.READ_ONLY, 0, Files.size(storageIndexesPath), Arena.global());

            long i = 0;
            var indexesNumber = indexesSegment.byteSize() / Long.BYTES;

            while (i <= indexesNumber) {
                var offset = indexesSegment.get(ValueLayout.JAVA_LONG_UNALIGNED, i * Long.BYTES);
                var savedKeySize = storageSegment.get(ValueLayout.JAVA_LONG_UNALIGNED, offset);
                offset += Long.BYTES;
                if (savedKeySize == key.byteSize()) {
                    var savedKey = storageSegment.asSlice(offset, savedKeySize);
                    offset += savedKeySize;
                    if (AbstractMemorySegmentDao.compareSegments(savedKey, key) == 0) {
                        var valueSize = storageSegment.get(ValueLayout.JAVA_LONG_UNALIGNED, offset);
                        offset += Long.BYTES;
                        MemorySegment savedValue = storageSegment.asSlice(offset, valueSize);
                        return new BaseEntry<>(savedKey, savedValue);
                    }
                }
                i++;
            }
            return null;

        } catch (IOException e) {
            return null;
        }
    }

    public void writeToFile(Collection<Entry<MemorySegment>> entries) throws IOException {
        if (entries.isEmpty()) {
            return;
        }

        long indexesSize = (long) entries.size() * Long.BYTES;
        long storageSize = 0;
        for (Entry<MemorySegment> entry : entries) {
            storageSize += entry.key().byteSize() + entry.value().byteSize() + 2L * Long.BYTES;
        }

        try (var storageChannel = FileChannel.open(storagePath, TRUNCATE_EXISTING, CREATE, WRITE, READ);
             var indexesChannel = FileChannel.open(storageIndexesPath, TRUNCATE_EXISTING, CREATE, WRITE, READ)) {
            var storageSegment = storageChannel.map(
                    FileChannel.MapMode.READ_WRITE, 0, storageSize, Arena.global());
            var indexesSegment = indexesChannel.map(
                    FileChannel.MapMode.READ_WRITE, 0, indexesSize, Arena.global());
            long indexOffset = 0;
            long storageRecordOffset = 0;
            for (Entry<MemorySegment> entry : entries) {
                indexesSegment.set(ValueLayout.JAVA_LONG_UNALIGNED, indexOffset, storageRecordOffset);
                indexOffset += Long.BYTES;

                storageRecordOffset += writeEntity(entry.key(), storageSegment, storageRecordOffset);
                storageRecordOffset += writeEntity(entry.value(), storageSegment, storageRecordOffset);
            }
        }
    }

    private long writeEntity(MemorySegment entityToWrite, MemorySegment storage, long storageOffset) {
        long entityToWriteSize = entityToWrite.byteSize();
        storage.set(ValueLayout.JAVA_LONG_UNALIGNED, storageOffset, entityToWriteSize);
        MemorySegment mappedRec = storage.asSlice(storageOffset + Long.BYTES, entityToWriteSize);
        mappedRec.copyFrom(entityToWrite);
        return entityToWriteSize + Long.BYTES;
    }
}
