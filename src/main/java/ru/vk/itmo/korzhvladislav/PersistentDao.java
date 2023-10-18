package ru.vk.itmo.korzhvladislav;

import ru.vk.itmo.BaseEntry;
import ru.vk.itmo.Entry;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Set;

import static java.lang.foreign.ValueLayout.JAVA_LONG_UNALIGNED;
import static java.nio.channels.FileChannel.MapMode.READ_WRITE;
import static java.nio.file.StandardOpenOption.READ;

// SSTable
public class PersistentDao extends InMemoryDao {
    private final Path dataFilePath;
    private final Arena arena = Arena.ofConfined();
    private static final String DATA_FILE_NAME = "sstable.txt";
    private static final Set<StandardOpenOption> READ_WRITE_OPTIONS = Set.of(
            READ,
            StandardOpenOption.WRITE,
            StandardOpenOption.CREATE,
            StandardOpenOption.TRUNCATE_EXISTING);

    public PersistentDao(Path path) throws UncheckedIOException {
        dataFilePath = path.resolve(DATA_FILE_NAME);
    }

    @Override
    public Entry<MemorySegment> get(MemorySegment key) {
        Entry<MemorySegment> entry = getDataStore().get(key);
        if (entry != null) {
            return entry;
        }
        return searchKey(key);
    }

    private Entry<MemorySegment> searchKey(MemorySegment key) {
        long offset = 0;
        MemorySegment dataSegment;
        try (FileChannel dataChannel = FileChannel.open(dataFilePath, READ)) {
            dataSegment = dataChannel.map(FileChannel.MapMode.READ_ONLY, 0, Files.size(dataFilePath), arena);
        } catch (IOException e) {
            return null;
        }
        while (offset < dataSegment.byteSize()) {
            long keySize;
            long valueSize;
            keySize = dataSegment.get(JAVA_LONG_UNALIGNED, offset);
            offset += keySize + Long.BYTES;
            valueSize = dataSegment.get(JAVA_LONG_UNALIGNED, offset);
            if (dataSegment.mismatch(key) == -1) {
                return new BaseEntry<>(key, dataSegment.asSlice(offset + Long.BYTES, valueSize));
            }
            offset += valueSize + Long.BYTES;
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
