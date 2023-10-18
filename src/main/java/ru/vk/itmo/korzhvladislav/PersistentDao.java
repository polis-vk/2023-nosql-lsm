package ru.vk.itmo.korzhvladislav;

import ru.vk.itmo.BaseEntry;
import ru.vk.itmo.Entry;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Set;

import static java.lang.foreign.ValueLayout.JAVA_LONG_UNALIGNED;
import static java.nio.channels.FileChannel.MapMode.READ_WRITE;
import static java.nio.file.StandardOpenOption.READ;

// SSTable
public class PersistentDao extends InMemoryDao {
    private final Path dataFilePath;
    private final Arena arena;
    private final MemorySegment memorySegment;
    private static final String DATA_FILE_NAME = "sstable.txt";
    private static final Set<StandardOpenOption> READ_WRITE_OPTIONS = Set.of(
            READ,
            StandardOpenOption.WRITE,
            StandardOpenOption.CREATE,
            StandardOpenOption.TRUNCATE_EXISTING);

    public PersistentDao(Path path) throws IOException {
        dataFilePath = path.resolve(DATA_FILE_NAME);
        arena = Arena.ofConfined();

        long size;
        try {
            size = Files.size(dataFilePath);
        } catch (NoSuchFileException e) {
            memorySegment = null;
            return;
        }

        boolean created = false;
        MemorySegment pageCurrent;
        try (FileChannel fileChannel = FileChannel.open(dataFilePath, READ)) {
            pageCurrent = fileChannel.map(FileChannel.MapMode.READ_ONLY, 0, size, arena);
            created = true;
        } catch (FileNotFoundException e) {
            pageCurrent = null;
        } finally {
            if (!created) {
                arena.close();
            }
        }

        memorySegment = pageCurrent;
    }

    @Override
    public Entry<MemorySegment> get(MemorySegment key) {
        Entry<MemorySegment> entry = super.get(key);
        if (entry != null) {
            return entry;
        }
        return searchKey(key);
    }

    private Entry<MemorySegment> searchKey(MemorySegment key) {
        long offset = 0;
        while (offset < memorySegment.byteSize()) {
            long keySize = memorySegment.get(JAVA_LONG_UNALIGNED, offset);
            offset += Long.BYTES;
            long valueSize = memorySegment.get(JAVA_LONG_UNALIGNED, offset);
            offset += Long.BYTES;

            if (keySize != key.byteSize()) {
                offset += keySize + valueSize;
                continue;
            }

            long mismatch = MemorySegment.mismatch(memorySegment, offset, offset + key.byteSize(),
                    key,
                    0,
                    key.byteSize());
            if (mismatch == -1) {
                MemorySegment slice = memorySegment.asSlice(offset + keySize, valueSize);
                return new BaseEntry<>(key, slice);
            }
            offset += keySize + valueSize;
        }
        return null;
    }

    @Override
    public void flush() throws IOException {
        long totalSize = getDataStore().values()
                .stream()
                .mapToLong(entry -> entry.key().byteSize() + entry.value().byteSize())
                .sum();
        long overheadSize = 2L * getDataStore().size() * Long.BYTES;

        long storageSize = totalSize + overheadSize;

        try (FileChannel dataChannel = FileChannel.open(dataFilePath, READ_WRITE_OPTIONS)) {
            try (Arena writingArena = Arena.ofConfined()) {
                MemorySegment dataSegmentWriter = dataChannel.map(READ_WRITE, 0, storageSize, writingArena);
                save(dataSegmentWriter);
                dataSegmentWriter.load();
            }
        }
    }

    private void save(MemorySegment dataSegmentWriter) {
        long offset = 0;
        for (Entry<MemorySegment> entry : getDataStore().values()) {
            dataSegmentWriter.set(JAVA_LONG_UNALIGNED, offset, entry.key().byteSize());
            offset += Long.BYTES;

            MemorySegment.copy(entry.key(), 0, dataSegmentWriter, offset, entry.key().byteSize());
            offset += entry.key().byteSize();

            dataSegmentWriter.set(JAVA_LONG_UNALIGNED, offset, entry.value().byteSize());
            offset += Long.BYTES;

            MemorySegment.copy(entry.value(), 0, dataSegmentWriter, offset, entry.value().byteSize());
            offset += entry.value().byteSize();
        }
    }

    @Override
    public void close() throws IOException {
        if (arena.scope().isAlive()) {
            arena.close();
        }
        flush();
    }
}
